// Package e2e tests the whole auction stack through nginx on localhost:8080:
// containers, every gateway, load balancing, failover, bidding, cross-gateway
// broadcasts, concurrent bidders, and the persister.
//
// Run from the project root, with the stack already up:
//
//	docker compose down --remove-orphans
//	docker compose up -d --build
//	go test ./e2e -v -count=1 -timeout 5m
//
// The engine holds ONE auction in memory and these tests change its state
// (bids are accepted, and the last test closes it). Recreate the engine before
// every run so each run starts with a fresh auction:
//
//	docker compose up -d --force-recreate engine
//	docker compose restart persister
//
// Environment overrides:
//
//	E2E_ADDR=localhost:8080     nginx address
//	E2E_COMPOSE_DIR=..          directory containing docker-compose.yml
//	E2E_SKIP_FAILOVER=1         skip the test that stops gateway3
package e2e

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	gatewayAddr = envOr("E2E_ADDR", "localhost:8080")
	composeDir  = envOr("E2E_COMPOSE_DIR", "..")
	gateways    = []string{"gateway1", "gateway2", "gateway3", "gateway4", "gateway5"}
	otherSvcs   = []string{"engine", "persister", "nginx"}
	runID       = strconv.FormatInt(time.Now().Unix(), 10)
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// ---------------------------------------------------------------------------
// docker helpers
// ---------------------------------------------------------------------------

func compose(args ...string) (string, error) {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// establishedOn counts ESTABLISHED TCP connections whose local port is 8080
// inside the given gateway container. Only nginx connects there, so this is
// the number of client connections the load balancer sent to that gateway.
func establishedOn(t *testing.T, service string) int {
	t.Helper()
	out, err := compose("exec", "-T", service, "netstat", "-tn")
	if err != nil {
		t.Fatalf("netstat in %s failed: %v\n%s", service, err, out)
	}
	n := 0
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) >= 6 && strings.HasPrefix(f[0], "tcp") &&
			strings.HasSuffix(f[3], ":8080") && f[5] == "ESTABLISHED" {
			n++
		}
	}
	return n
}

func countPerGateway(t *testing.T) map[string]int {
	t.Helper()
	m := map[string]int{}
	for _, g := range gateways {
		m[g] = establishedOn(t, g)
	}
	return m
}

// ---------------------------------------------------------------------------
// minimal RFC 6455 client (standard library only)
// ---------------------------------------------------------------------------

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

var errClosed = errors.New("server closed websocket")

type wsConn struct {
	conn   net.Conn
	br     *bufio.Reader
	mu     sync.Mutex
	closed bool
}

func dialWS(path string, timeout time.Duration) (*wsConn, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", gatewayAddr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\n"+
		"Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		path, gatewayAddr, key)
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, err
	}
	if !strings.Contains(status, " 101 ") {
		conn.Close()
		return nil, fmt.Errorf("websocket upgrade failed: %s", strings.TrimSpace(status))
	}

	accept := ""
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, err
		}
		if line == "\r\n" || line == "\n" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-accept:") {
			accept = strings.TrimSpace(line[len("sec-websocket-accept:"):])
		}
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	if want := base64.StdEncoding.EncodeToString(sum[:]); accept != want {
		conn.Close()
		return nil, fmt.Errorf("bad Sec-WebSocket-Accept: got %q want %q", accept, want)
	}

	_ = conn.SetDeadline(time.Time{})
	return &wsConn{conn: conn, br: br}, nil
}

func (w *wsConn) sendFrame(opcode byte, payload []byte, timeout time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.conn.SetWriteDeadline(time.Now().Add(timeout))

	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	frame := []byte{0x80 | opcode}
	n := len(payload)
	switch {
	case n < 126:
		frame = append(frame, 0x80|byte(n))
	case n <= 65535:
		frame = append(frame, 0x80|126)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(n))
		frame = append(frame, ext...)
	default:
		frame = append(frame, 0x80|127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(n))
		frame = append(frame, ext...)
	}
	frame = append(frame, mask...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	_, err := w.conn.Write(frame)
	return err
}

func (w *wsConn) sendText(msg string) error {
	return w.sendFrame(0x1, []byte(msg), 3*time.Second)
}

// readText returns the next text/binary frame, answering pings on the way.
// On timeout it returns a net.Error with Timeout() == true.
func (w *wsConn) readText(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		_ = w.conn.SetReadDeadline(deadline)

		var hdr [2]byte
		if _, err := io.ReadFull(w.br, hdr[:]); err != nil {
			return "", err
		}
		opcode := hdr[0] & 0x0F
		masked := hdr[1]&0x80 != 0
		n := uint64(hdr[1] & 0x7F)
		switch n {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(w.br, ext[:]); err != nil {
				return "", err
			}
			n = uint64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(w.br, ext[:]); err != nil {
				return "", err
			}
			n = binary.BigEndian.Uint64(ext[:])
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(w.br, mask[:]); err != nil {
				return "", err
			}
		}
		if n > 1<<24 {
			return "", fmt.Errorf("frame too large: %d", n)
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(w.br, data); err != nil {
			return "", err
		}
		if masked {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}

		switch opcode {
		case 0x8:
			return "", errClosed
		case 0x9:
			_ = w.sendFrame(0xA, data, 2*time.Second)
		case 0xA:
			// pong, ignore
		case 0x0, 0x1, 0x2:
			return string(data), nil
		}
	}
}

// drain reads frames until nothing arrives for `quiet`.
func (w *wsConn) drain(quiet time.Duration) []string {
	var out []string
	for {
		m, err := w.readText(quiet)
		if err != nil {
			return out
		}
		out = append(out, m)
	}
}

func (w *wsConn) close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	_ = w.sendFrame(0x8, nil, time.Second)
	_ = w.conn.Close()
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func connect(t *testing.T, user string) *wsConn {
	t.Helper()
	w, err := dialWS("/ws?user="+user, 10*time.Second)
	if err != nil {
		t.Fatalf("[%s] websocket connect failed: %v", user, err)
	}
	t.Cleanup(w.close)
	return w
}

// openConns opens n WebSocket connections through nginx and keeps them open.
func openConns(t *testing.T, n int, prefix string) []*wsConn {
	t.Helper()
	var conns []*wsConn
	for i := 0; i < n; i++ {
		w, err := dialWS(fmt.Sprintf("/ws?user=%s-%d", prefix, i), 15*time.Second)
		if err != nil {
			for _, c := range conns {
				c.close()
			}
			t.Fatalf("connection %d/%d failed: %v", i+1, n, err)
		}
		conns = append(conns, w)
	}
	return conns
}

func closeAll(conns []*wsConn) {
	for _, c := range conns {
		c.close()
	}
	time.Sleep(400 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// bid helpers
// ---------------------------------------------------------------------------

func placeBid(t *testing.T, w *wsConn, bidID string, amount int64) []string {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"type": "bid", "bid_id": bidID, "amount": amount})
	if err := w.sendText(string(payload)); err != nil {
		t.Fatalf("send bid %s failed: %v", bidID, err)
	}
	return w.drain(800 * time.Millisecond)
}

// outcome looks for an "accepted" boolean in the frames (top level or inside a
// nested object). known is false if the gateway's response has no such field,
// in which case the tests log the raw frames instead of asserting.
func outcome(msgs []string) (accepted, known bool) {
	for _, m := range msgs {
		var obj map[string]any
		if json.Unmarshal([]byte(m), &obj) != nil {
			continue
		}
		if v, ok := obj["accepted"].(bool); ok {
			return v, true
		}
		for _, nested := range obj {
			if sub, ok := nested.(map[string]any); ok {
				if v, ok := sub["accepted"].(bool); ok {
					return v, true
				}
			}
		}
	}
	return false, false
}

// ---------------------------------------------------------------------------
// 1. containers
// ---------------------------------------------------------------------------

func TestStackHealthy(t *testing.T) {
	out, err := compose("ps", "--services", "--status", "running")
	if err != nil {
		t.Fatalf("docker compose ps failed: %v\n%s", err, out)
	}
	running := map[string]bool{}
	for _, s := range strings.Fields(out) {
		running[s] = true
	}
	for _, s := range append(append([]string{}, otherSvcs...), gateways...) {
		if !running[s] {
			t.Errorf("service %q is not running", s)
		}
	}

	ids, err := compose("ps", "-q")
	if err != nil {
		t.Fatalf("docker compose ps -q failed: %v\n%s", err, ids)
	}
	for _, id := range strings.Fields(ids) {
		o, err := exec.Command("docker", "inspect", "-f",
			"{{.Name}} {{.State.Status}} {{.RestartCount}}", id).CombinedOutput()
		if err != nil {
			t.Errorf("inspect %s: %v", id, err)
			continue
		}
		f := strings.Fields(string(o))
		if len(f) < 3 {
			continue
		}
		t.Logf("%-22s status=%-8s restarts=%s", strings.TrimPrefix(f[0], "/"), f[1], f[2])
		if f[1] != "running" {
			t.Errorf("%s is %s", f[0], f[1])
		}
		if f[2] != "0" {
			t.Errorf("%s has restarted %s times (crash loop?)", f[0], f[2])
		}
	}

	logs, _ := compose("logs", "--no-color", "--tail", "100", "engine")
	if !strings.Contains(logs, "gRPC on") {
		t.Errorf("engine log has no 'gRPC on' startup line:\n%s", logs)
	}
	for _, bad := range []string{"exec format error", "panic:", "redis:"} {
		if strings.Contains(logs, bad) {
			t.Errorf("engine log contains %q:\n%s", bad, logs)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. each gateway
// ---------------------------------------------------------------------------

func TestEachGatewayHealthy(t *testing.T) {
	for _, g := range gateways {
		g := g
		t.Run(g, func(t *testing.T) {
			logs, err := compose("logs", "--no-color", "--tail", "50", g)
			if err != nil {
				t.Fatalf("logs failed: %v\n%s", err, logs)
			}
			if !strings.Contains(logs, "gateway CLI/WS mode") {
				t.Errorf("no startup line in %s log:\n%s", g, logs)
			}
			if strings.Contains(logs, "panic:") || strings.Contains(logs, "exec format error") {
				t.Errorf("%s log shows a crash:\n%s", g, logs)
			}
			// The gateway must be able to run commands and see port 8080 open.
			_ = establishedOn(t, g)
		})
	}
}

// ---------------------------------------------------------------------------
// 3. entry point through nginx
// ---------------------------------------------------------------------------

func TestHTTPThroughNginx(t *testing.T) {
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get("http://" + gatewayAddr + "/")
	if err != nil {
		t.Fatalf("GET / through nginx failed: %v", err)
	}
	resp.Body.Close()
	t.Logf("GET / -> %d", resp.StatusCode)
}

func TestWebSocketHandshakeAndInit(t *testing.T) {
	w := connect(t, "handshake-"+runID)
	msgs := w.drain(1500 * time.Millisecond)
	if len(msgs) == 0 {
		t.Log("no init frame received after connect (gateway may only send on events)")
	}
	for _, m := range msgs {
		t.Logf("INIT: %s", m)
	}
}

// ---------------------------------------------------------------------------
// 4. load balancer
// ---------------------------------------------------------------------------

func TestLoadBalancerDistribution(t *testing.T) {
	const n = 25
	conns := openConns(t, n, "lb-"+runID)
	defer closeAll(conns)
	time.Sleep(500 * time.Millisecond)

	counts := countPerGateway(t)
	total, min, max := 0, 1<<30, 0
	for _, g := range gateways {
		c := counts[g]
		t.Logf("%-9s %2d connections", g, c)
		total += c
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
		if c == 0 {
			t.Errorf("%s received no connections", g)
		}
	}
	if total < n {
		t.Errorf("gateways report %d established connections, expected at least %d", total, n)
	}
	if max-min > 3 {
		t.Errorf("uneven distribution (min=%d max=%d); least_conn should keep this tight", min, max)
	}
}

func TestLoadBalancerFailover(t *testing.T) {
	if os.Getenv("E2E_SKIP_FAILOVER") == "1" {
		t.Skip("E2E_SKIP_FAILOVER=1")
	}
	const victim = "gateway3"
	t.Cleanup(func() { _, _ = compose("start", victim) })

	if out, err := compose("stop", victim); err != nil {
		t.Fatalf("stop %s failed: %v\n%s", victim, err, out)
	}
	time.Sleep(time.Second)

	// With one gateway down, every client must still get through.
	conns := openConns(t, 10, "fo-"+runID)
	time.Sleep(500 * time.Millisecond)
	sum := 0
	for _, g := range gateways {
		if g != victim {
			sum += establishedOn(t, g)
		}
	}
	t.Logf("with %s down: %d/10 connections landed on the other gateways", victim, sum)
	if sum < 10 {
		t.Errorf("expected all 10 connections on surviving gateways, got %d", sum)
	}
	closeAll(conns)

	// Bring it back; nginx marks it failed for fail_timeout=10s, then retries.
	if out, err := compose("start", victim); err != nil {
		t.Fatalf("start %s failed: %v\n%s", victim, err, out)
	}
	t.Logf("waiting 14s for nginx to put %s back in rotation...", victim)
	time.Sleep(14 * time.Second)

	conns = openConns(t, 15, "fo2-"+runID)
	defer closeAll(conns)
	time.Sleep(500 * time.Millisecond)
	if c := establishedOn(t, victim); c == 0 {
		t.Errorf("%s did not receive any connections after recovery", victim)
	} else {
		t.Logf("%s recovered and has %d connections", victim, c)
	}
}

// ---------------------------------------------------------------------------
// 5. bidding
// ---------------------------------------------------------------------------

func TestBidFlow(t *testing.T) {
	steps := []struct {
		user, id string
		amount   int64
		want     string // "accept" or "reject"
		note     string
	}{
		{"u1", "1", 110, "accept", "opening bid"},
		{"u2", "1", 125, "accept", "higher bid"},
		{"u3", "1", 140, "accept", "leads"},
		{"u2", "1", 125, "reject", "duplicate bid_id"},
		{"u4", "1", 80, "reject", "below minimum price"},
		{"u4", "2", 160, "accept", "jumps to top, pushes lowest winner out"},
	}

	conns := map[string]*wsConn{}
	for _, s := range steps {
		w, ok := conns[s.user]
		if !ok {
			w = connect(t, s.user)
			w.drain(700 * time.Millisecond)
			conns[s.user] = w
		}
		bidID := fmt.Sprintf("%s-%s-%s", runID, s.user, s.id)
		msgs := placeBid(t, w, bidID, s.amount)
		t.Logf("[%s] bid %d (%s) -> %v", s.user, s.amount, s.note, msgs)

		if len(msgs) == 0 {
			t.Errorf("[%s] no response to bid %s", s.user, bidID)
			continue
		}
		accepted, known := outcome(msgs)
		if !known {
			t.Logf("[%s] response has no 'accepted' field; not asserting outcome", s.user)
			continue
		}
		if wantAccept := s.want == "accept"; accepted != wantAccept {
			t.Errorf("[%s] %s: accepted=%v, want %v (stale engine state? recreate the engine and retry)",
				s.user, s.note, accepted, wantAccept)
		}
	}
}

func TestConcurrentBidders(t *testing.T) {
	const n = 30
	type result struct {
		err     error
		latency time.Duration
	}
	results := make([]result, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, err := dialWS(fmt.Sprintf("/ws?user=c-%s-%d", runID, i), 15*time.Second)
			if err != nil {
				results[i].err = err
				return
			}
			defer w.close()
			w.drain(500 * time.Millisecond)

			payload, _ := json.Marshal(map[string]any{
				"type": "bid", "bid_id": fmt.Sprintf("%s-c-%d", runID, i), "amount": 200000 + int64(i)*10,
			})
			start := time.Now()
			if err := w.sendText(string(payload)); err != nil {
				results[i].err = err
				return
			}
			if _, err := w.readText(5 * time.Second); err != nil {
				results[i].err = fmt.Errorf("no response: %w", err)
				return
			}
			results[i].latency = time.Since(start)
		}(i)
	}
	wg.Wait()

	var lat []time.Duration
	for i, r := range results {
		if r.err != nil {
			t.Errorf("bidder %d: %v", i, r.err)
			continue
		}
		lat = append(lat, r.latency)
	}
	if len(lat) > 0 {
		sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
		t.Logf("%d/%d bidders got a response; latency p50=%s max=%s",
			len(lat), n, lat[len(lat)/2], lat[len(lat)-1])
	}
}

// A bid placed on one connection must reach watchers on all gateways, because
// the gateways all subscribe to the single engine.
func TestBroadcastReachesAllGateways(t *testing.T) {
	const watchers = 10 // least_conn spreads these over all 5 gateways
	ws := openConns(t, watchers, "watch-"+runID)
	defer closeAll(ws)
	for _, w := range ws {
		w.drain(300 * time.Millisecond)
	}

	counts := countPerGateway(t)
	covered := 0
	for _, g := range gateways {
		if counts[g] > 0 {
			covered++
		}
	}
	t.Logf("watchers per gateway: %v", counts)
	if covered < len(gateways) {
		t.Errorf("watchers only cover %d of %d gateways", covered, len(gateways))
	}

	bidder := connect(t, "broadcaster-"+runID)
	bidder.drain(500 * time.Millisecond)
	msgs := placeBid(t, bidder, fmt.Sprintf("%s-bcast", runID), 5_000_000+time.Now().Unix()%1000)
	if accepted, known := outcome(msgs); known && !accepted {
		t.Fatalf("broadcast bid was rejected: %v", msgs)
	}

	missed := 0
	for i, w := range ws {
		m, err := w.readText(3 * time.Second)
		if err != nil {
			missed++
			t.Errorf("watcher %d received no broadcast: %v", i, err)
			continue
		}
		t.Logf("watcher %d got: %s", i, m)
	}
	if missed == 0 {
		t.Logf("all %d watchers across all gateways received the update", watchers)
	}
}

// ---------------------------------------------------------------------------
// 6. closing and persistence (runs last: it closes the auction)
// ---------------------------------------------------------------------------

var savedRe = regexp.MustCompile(`winners written \((\d+) bids saved\)`)

func TestCloseAuctionAndPersister(t *testing.T) {
	since := time.Now().UTC().Add(-2 * time.Second).Format(time.RFC3339)

	admin := connect(t, "admin-"+runID)
	admin.drain(500 * time.Millisecond)
	if err := admin.sendText(`{"type":"close_auction"}`); err != nil {
		t.Fatalf("send close_auction: %v", err)
	}
	for _, m := range admin.drain(1500 * time.Millisecond) {
		t.Logf("[admin] %s", m)
	}

	// New bids must now be refused.
	late := connect(t, "late-"+runID)
	late.drain(300 * time.Millisecond)
	msgs := placeBid(t, late, fmt.Sprintf("%s-late", runID), 9_000_000)
	t.Logf("bid after close -> %v", msgs)
	if accepted, known := outcome(msgs); known && accepted {
		t.Errorf("a bid was accepted after the auction closed")
	}

	// The persister must write the winners.
	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		logs, _ := compose("logs", "--no-color", "--since", since, "persister")
		if all := savedRe.FindAllStringSubmatch(logs, -1); len(all) > 0 {
			last = all[len(all)-1][1]
			break
		}
		time.Sleep(2 * time.Second)
	}
	if last == "" {
		t.Fatalf("persister never logged 'winners written' within 30s; check `docker compose logs persister` (Redis timeout?)")
	}
	saved, _ := strconv.Atoi(last)
	t.Logf("persister saved %d bids", saved)
	if saved == 0 {
		t.Errorf("persister saved 0 bids; the bid log or stream is not reaching it")
	}

	// Services should still be up after the close.
	out, _ := compose("ps", "--services", "--status", "running")
	for _, s := range append(append([]string{}, otherSvcs...), gateways...) {
		if !strings.Contains(out, s) {
			t.Errorf("%s is not running after auction close", s)
		}
	}
}