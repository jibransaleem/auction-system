"""
Locust load test for the auction stack (nginx -> 5 gateways -> engine).

What it does
------------
- Opens real WebSocket connections through nginx (/ws?user=...) and places bids.
- Sends at most MAX_REQUESTS bids in total (default 50,000).
- Follows the auction clock: normal traffic for most of the run, then a SPIKE in
  the last SPIKE_SECONDS (default 10) before the auction ends. Spike users are
  connected beforehand, so the spike is a burst of bids, not a burst of
  connection setups.
- Reserves part of the request budget (SPIKE_RESERVE) for the spike so the
  steady phase cannot use it all up.
- Writes a per-second timeline CSV and prints a phase-by-phase summary with
  second-by-second detail for the spike, on top of Locust's own CSV/HTML.

How it finds the end of the auction
-----------------------------------
1. AUCTION_END env var (RFC3339 or unix seconds), else
2. the engine's startup log line ("... ends at <RFC3339>, gRPC on ..."), read
   with `docker compose logs engine` (works when running natively), else
3. fallback: RUN_TIME seconds after this process started (a warning is printed;
   this will NOT line up with the real auction end).

Run it:

    locust -f loadtest/locustfile.py --headless --csv results/run --html results/report.html

Merge per-process timelines:

    python loadtest/locustfile.py merge results/timeline_*.csv
"""

import csv
import glob
import json
import math
import os
import random
import re
import socket
import subprocess
import sys
import time
from datetime import datetime

# locust must be imported before websocket so gevent monkey-patching happens first.
from locust import LoadTestShape, User, constant, events, task
from locust.runners import WorkerRunner
import gevent
from gevent.event import Event
from gevent.lock import RLock
import websocket  # pip install websocket-client


# ---------------------------------------------------------------------------
# configuration (all overridable through environment variables)
# ---------------------------------------------------------------------------

def _int(k, d):
    return int(os.environ.get(k, d))


def _float(k, d):
    return float(os.environ.get(k, d))


GATEWAY_ADDR = os.environ.get("GATEWAY_ADDR", "localhost:8080")
MAX_REQUESTS = _int("MAX_REQUESTS", 50_000)      # total bids across ALL processes
PROCESSES = _int("PROCESSES", 1)                 # must equal locust --processes
SPIKE_RESERVE = _int("SPIKE_RESERVE", 25_000)    # bids kept back for the final spike
SPIKE_S = _float("SPIKE_SECONDS", 10)            # length of the spike window
PRE_SPIKE_S = _float("PRE_SPIKE_SECONDS", 25)    # spike users connect this long before the end
RAMP_S = _float("RAMP_SECONDS", 20)
TAIL_S = _float("TAIL_SECONDS", 5)               # keep running after the auction ends
MIN_LEAD_S = _float("MIN_LEAD_SECONDS", 30)      # refuse to start if less time is left
BASE_USERS = _int("BASE_USERS", 100)
SPIKE_USERS = _int("SPIKE_USERS", 1000)
BASE_THINK = (_float("BASE_THINK_MIN", 0.5), _float("BASE_THINK_MAX", 1.5))
SPIKE_THINK = (_float("SPIKE_THINK_MIN", 0.005), _float("SPIKE_THINK_MAX", 0.03))
MIN_PRICE = _int("MIN_PRICE", 100)
MIN_INCREMENT = _int("MIN_INCREMENT", 10)
LOW_BID_SHARE = _float("LOW_BID_SHARE", 0.12)    # bids below MIN_PRICE (should be rejected)
DUP_BID_SHARE = _float("DUP_BID_SHARE", 0.08)    # repeated bid_id (should be rejected)
BID_TIMEOUT = _float("BID_TIMEOUT", 5.0)
CONNECT_TIMEOUT = _float("CONNECT_TIMEOUT", 10.0)
RUN_TIME = _float("RUN_TIME", 120)
RESULTS_DIR = os.environ.get("RESULTS_DIR", "results")
RUN_TAG = os.environ.get("RUN_TAG", str(int(time.time())))

PROC_MAX = math.ceil(MAX_REQUESTS / max(1, PROCESSES))
PROC_STEADY = math.ceil(max(0, MAX_REQUESTS - SPIKE_RESERVE) / max(1, PROCESSES))


# ---------------------------------------------------------------------------
# target & auction end detection
# ---------------------------------------------------------------------------

def _split_host_port(addr):
    addr = addr.strip()
    if addr.startswith("http://"):
        addr = addr[len("http://"):]
    elif addr.startswith("https://"):
        addr = addr[len("https://"):]
    addr = addr.split("/", 1)[0]
    if ":" in addr:
        host, port_s = addr.rsplit(":", 1)
        return host, int(port_s)
    return addr, 80


def check_target_reachable(addr, timeout=2.0):
    host, port = _split_host_port(addr)
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, None
    except Exception as e:
        return False, e


def _parse_ts(s):
    s = s.strip()
    try:
        return float(s)
    except ValueError:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


_END_RE = re.compile(r"ends at (\d{4}-\d{2}-\d{2}T[0-9:.]+(?:Z|[+-]\d{2}:\d{2}))")


def detect_auction_end():
    v = os.environ.get("AUCTION_END")
    if v:
        return _parse_ts(v), "AUCTION_END env var"
    try:
        out = subprocess.run(
            ["docker", "compose", "logs", "--no-color", "--tail", "200", "engine"],
            cwd=os.environ.get("COMPOSE_DIR", "."),
            capture_output=True, text=True, timeout=20,
        ).stdout
        found = _END_RE.findall(out)
        if found:
            return _parse_ts(found[-1]), "engine log"
    except Exception:
        pass
    return time.time() + RUN_TIME, "FALLBACK (not the real auction end!)"


AUCTION_END, END_SOURCE = detect_auction_end()


# ---------------------------------------------------------------------------
# shared per-process state
# ---------------------------------------------------------------------------

class State:
    def __init__(self):
        self.lock = RLock()
        self.t0 = time.time()
        self.sent = 0
        self.steady_sent = 0
        self.price = MIN_PRICE
        self.last_amount = MIN_PRICE
        self.broadcasts = 0
        self.connect_ok = 0
        self.connect_fail = 0
        self.user_seq = 0
        self.buckets = {}   # second since t0 -> counters
        self.outcomes = {}  # (kind, outcome) -> count


S = State()


def phase_at(now, t0=None):
    """ramp / steady / prespike / spike / tail, based on time left in the auction."""
    with S.lock:
        t0 = S.t0 if t0 is None else t0
        rem = AUCTION_END - now
    if rem < 0:
        return "tail"
    if rem <= SPIKE_S:
        return "spike"
    if rem <= PRE_SPIKE_S:
        return "prespike"
    if now - t0 < RAMP_S:
        return "ramp"
    return "steady"


def record(ok, kind, outcome, latency_ms):
    now = time.time()
    with S.lock:
        sec = int(now - S.t0)
        b = S.buckets.setdefault(sec, {
            "sent": 0, "accepted": 0, "rejected": 0, "timeouts": 0, "lat": []
        })
        b["sent"] += 1
        if ok:
            b["accepted" if outcome == "accepted" else "rejected"] += 1
            b["lat"].append(latency_ms)
        else:
            b["timeouts"] += 1
        key = (kind, outcome)
        S.outcomes[key] = S.outcomes.get(key, 0) + 1


# ---------------------------------------------------------------------------
# message helpers
# ---------------------------------------------------------------------------

def parse(raw):
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def find_key(obj, key):
    """Value of `key` at the top level or one dict deeper (e.g. {"result": {...}})."""
    if key in obj:
        return obj[key]
    for v in obj.values():
        if isinstance(v, dict) and key in v:
            return v[key]
    return None


# ---------------------------------------------------------------------------
# the simulated bidder
# ---------------------------------------------------------------------------

class AuctionBidder(User):
    wait_time = constant(0)  # pacing is done inside the task (spike-aware)

    def on_start(self):
        with S.lock:
            S.user_seq += 1
            seq = S.user_seq
        self.uid = f"lt{os.getpid()}-{seq}"
        self.ws = None
        self.connected = False
        self.alive = True
        self.n = 0
        self.tail_sent = 0
        self.last_id = None
        self.last_amount = 0
        self.pending_id = None
        self.reply = None
        self.reply_evt = Event()

    def on_stop(self):
        self.alive = False
        ws = getattr(self, "ws", None)
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    # -- connection ---------------------------------------------------------

    def _fire(self, rtype, name, ms, length=0, exc=None):
        self.environment.events.request.fire(
            request_type=rtype, name=name, response_time=ms,
            response_length=length, exception=exc, context={})

    def _connect(self):
        url = f"ws://{GATEWAY_ADDR}/ws?user={self.uid}"
        start = time.perf_counter()
        try:
            ws = websocket.create_connection(url, timeout=CONNECT_TIMEOUT, suppress_origin=True)
            ws.settimeout(2)
        except Exception as e:
            with S.lock:
                S.connect_fail += 1
            self._fire("WS", "connect", (time.perf_counter() - start) * 1000, exc=e)
            return False
        with S.lock:
            S.connect_ok += 1
        self._fire("WS", "connect", (time.perf_counter() - start) * 1000)
        self.ws = ws
        self.connected = True
        gevent.spawn(self._reader, ws)
        return True

    def _reader(self, ws):
        """Reads every frame: matches the reply to our pending bid, counts the rest as broadcasts."""
        while getattr(self, "alive", False) and ws is getattr(self, "ws", None):
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                break
            if not raw:
                break
            obj = parse(raw)
            if obj is None:
                continue
            price = find_key(obj, "price")
            if isinstance(price, (int, float)):
                with S.lock:
                    if int(price) > S.price:
                        S.price = int(price)
            if self.pending_id is not None and self._is_reply(obj):
                self.reply = (obj, len(raw))
                self.reply_evt.set()
            else:
                with S.lock:
                    S.broadcasts += 1
        if ws is getattr(self, "ws", None):
            self.connected = False
        self.reply_evt.set()  # wake a sender that is waiting on a dead socket

    def _is_reply(self, obj):
        bid = find_key(obj, "bid_id")
        if bid is not None:
            return bid == self.pending_id
        return find_key(obj, "accepted") is not None

    # -- bidding ------------------------------------------------------------

    def _pick_kind(self):
        r = random.random()
        if r < LOW_BID_SHARE:
            return "low"
        if r < LOW_BID_SHARE + DUP_BID_SHARE and self.last_id:
            return "duplicate"
        return "competitive"

    def _may_send(self, ph):
        with S.lock:
            if S.sent >= PROC_MAX:
                return False
            if ph == "tail":
                return self.tail_sent < 1
            if ph != "spike" and S.steady_sent >= PROC_STEADY:
                return False
            return True

    def _do_bid(self, ph):
        kind = self._pick_kind()
        if kind == "duplicate":
            bid_id, amount = self.last_id, self.last_amount
        else:
            bid_id = f"{RUN_TAG}-{self.uid}-{self.n}"
            self.n += 1
            if kind == "low":
                amount = MIN_PRICE - 1
            else:
                with S.lock:
                    base_price = max(S.price, S.last_amount, self.last_amount)
                    amount = base_price + MIN_INCREMENT + random.randint(0, 3)
                    S.last_amount = amount

        with S.lock:
            S.sent += 1
            if ph != "spike":
                S.steady_sent += 1
        if ph == "tail":
            self.tail_sent += 1

        self.pending_id = bid_id
        self.reply = None
        self.reply_evt.clear()
        start = time.perf_counter()
        try:
            self.ws.send(json.dumps({"type": "bid", "bid_id": bid_id, "amount": amount}))
        except Exception as e:
            self.connected = False
            self.pending_id = None
            self._fail(kind, "send_error", start, e)
            return
        self.reply_evt.wait(BID_TIMEOUT)
        ms = (time.perf_counter() - start) * 1000
        self.pending_id = None

        if self.reply is None:
            why = "connection_lost" if not self.connected else "timeout"
            self._fail(kind, why, start, TimeoutError(why))
            return

        obj, length = self.reply
        accepted = find_key(obj, "accepted")
        reason = find_key(obj, "reason")
        if accepted is True:
            outcome = "accepted"
            self.last_id, self.last_amount = bid_id, amount
        elif accepted is False:
            outcome = f"rejected:{reason if reason not in (None, '') else 'unknown'}"
        else:
            outcome = "reply:unclassified"
        record(True, kind, outcome, ms)
        self._fire("WS", f"bid:{kind} -> {outcome}", ms, length)

    def _fail(self, kind, why, start, exc):
        ms = (time.perf_counter() - start) * 1000
        record(False, kind, why, ms)
        self._fire("WS", f"bid:{kind} -> {why}", ms, exc=exc)

    def _think(self):
        ph = phase_at(time.time())
        lo, hi = SPIKE_THINK if ph == "spike" else BASE_THINK
        end = time.time() + random.uniform(lo, hi)
        while True:
            left = end - time.time()
            if left <= 0:
                return
            gevent.sleep(min(left, 0.1))
            # cut a long normal-pace sleep short the moment the spike starts
            if ph != "spike" and phase_at(time.time()) == "spike":
                return

    @task
    def bid(self):
        if not getattr(self, "connected", False):
            if not self._connect():
                gevent.sleep(1)
                return
        ph = phase_at(time.time())
        if not self._may_send(ph):
            gevent.sleep(0.25)
            return
        self._do_bid(ph)
        self._think()


# ---------------------------------------------------------------------------
# load shape: ramp -> steady -> pre-connect spike users -> spike -> tail
# ---------------------------------------------------------------------------

class AuctionSpikeShape(LoadTestShape):
    def tick(self):
        now = time.time()
        rem = AUCTION_END - now
        run = self.get_run_time()

        if run < 1 and rem < MIN_LEAD_S:
            print(f"\n[loadtest] ABORT: only {rem:.0f}s left in the auction (need >= {MIN_LEAD_S:.0f}s). "
                  "Recreate the engine with a longer AUCTION_DURATION and refresh AUCTION_END.\n", flush=True)
            return None
        if rem < -TAIL_S:
            return None

        if rem <= PRE_SPIKE_S:
            # spike users join now at normal pace; they switch to rapid fire at rem <= SPIKE_S
            window = max(1.0, (PRE_SPIKE_S - SPIKE_S) * 0.6)
            return SPIKE_USERS, max(1.0, (SPIKE_USERS - BASE_USERS) / window)
        if run < RAMP_S:
            return max(1, math.ceil(BASE_USERS * run / RAMP_S)), max(1.0, BASE_USERS / RAMP_S)
        return BASE_USERS, 10


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------

FIELDS = ["t", "rel_to_end_s", "phase", "sent", "responses", "accepted", "rejected",
          "timeouts", "lat_sum_ms", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_max_ms"]
PHASE_ORDER = ["ramp", "steady", "prespike", "spike", "tail"]


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = max(0, min(len(sorted_vals) - 1, math.ceil(p / 100 * len(sorted_vals)) - 1))
    return sorted_vals[k]


def rows_from_buckets():
    rows = []
    with S.lock:
        secs = sorted(S.buckets)
        t0 = S.t0
        buckets_copy = {
            sec: {
                "sent": b["sent"],
                "accepted": b["accepted"],
                "rejected": b["rejected"],
                "timeouts": b["timeouts"],
                "lat": list(b["lat"]),
            }
            for sec, b in S.buckets.items()
        }
    for sec in secs:
        b = buckets_copy[sec]
        lat = sorted(b["lat"])
        abs_t = t0 + sec
        rows.append({
            "t": sec,
            "rel_to_end_s": round(abs_t - AUCTION_END, 1),
            "phase": phase_at(abs_t + 0.5, t0=t0),
            "sent": b["sent"],
            "responses": len(lat),
            "accepted": b["accepted"],
            "rejected": b["rejected"],
            "timeouts": b["timeouts"],
            "lat_sum_ms": round(sum(lat), 1),
            "lat_p50_ms": round(pct(lat, 50), 1),
            "lat_p95_ms": round(pct(lat, 95), 1),
            "lat_p99_ms": round(pct(lat, 99), 1),
            "lat_max_ms": round(lat[-1], 1) if lat else 0.0,
        })
    return rows


def write_csv(path, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


def summarize(rows, title):
    if not rows:
        print(f"[loadtest] {title}: no data")
        return
    groups = {}
    for r in rows:
        groups.setdefault(r["phase"], []).append(r)

    total_sent = sum(r["sent"] for r in rows)
    print(f"\n=== {title} ===")
    print(f"bids sent: {total_sent}  (cap {MAX_REQUESTS})   "
          f"accepted: {sum(r['accepted'] for r in rows)}   "
          f"rejected: {sum(r['rejected'] for r in rows)}   "
          f"timeouts/errors: {sum(r['timeouts'] for r in rows)}")
    print(f"{'phase':9} {'secs':>4} {'bids':>7} {'avg rps':>8} {'peak rps':>8} "
          f"{'avg ms':>8} {'worst p95':>9} {'worst p99':>9} {'max ms':>8} {'timeouts':>9}")
    for ph in PHASE_ORDER:
        g = groups.get(ph)
        if not g:
            continue
        secs = len(g)
        sent = sum(r["sent"] for r in g)
        resp = sum(r["responses"] for r in g)
        avg = sum(r["lat_sum_ms"] for r in g) / resp if resp else 0.0
        to = sum(r["timeouts"] for r in g)
        print(f"{ph:9} {secs:4d} {sent:7d} {sent / secs:8.1f} {max(r['sent'] for r in g):8d} "
              f"{avg:8.1f} {max(r['lat_p95_ms'] for r in g):9.1f} {max(r['lat_p99_ms'] for r in g):9.1f} "
              f"{max(r['lat_max_ms'] for r in g):8.1f} {to:5d} ({to / sent:5.1%})")

    steady = [r for r in rows if r["phase"] in ("steady", "prespike")]
    spike = groups.get("spike", [])
    if steady and spike:
        s_rps = sum(r["sent"] for r in steady) / len(steady)
        k_rps = sum(r["sent"] for r in spike) / len(spike)
        if s_rps:
            print(f"\nspike vs normal traffic: {k_rps:.0f} rps vs {s_rps:.0f} rps  (x{k_rps / s_rps:.1f})")

    if spike:
        print("\nfinal-window detail (second by second; seconds counted by when the reply arrived):")
        print(f"{'to end':>7} {'bids':>6} {'accept':>7} {'reject':>7} {'timeout':>7} "
              f"{'avg ms':>8} {'p50':>7} {'p95':>7} {'p99':>7} {'max':>8}")
        for r in spike:
            avg = r["lat_sum_ms"] / r["responses"] if r["responses"] else 0.0
            print(f"{r['rel_to_end_s']:7.1f} {r['sent']:6d} {r['accepted']:7d} {r['rejected']:7d} "
                  f"{r['timeouts']:7d} {avg:8.1f} {r['lat_p50_ms']:7.1f} {r['lat_p95_ms']:7.1f} "
                  f"{r['lat_p99_ms']:7.1f} {r['lat_max_ms']:8.1f}")
    elif groups:
        print("\nno bids landed in the spike window; check that AUCTION_END matches the engine's end time.")


@events.test_start.add_listener
def _on_test_start(environment, **kw):
    global AUCTION_END, END_SOURCE
    ok, err = check_target_reachable(GATEWAY_ADDR)
    with S.lock:
        S.t0 = time.time()
        if not os.environ.get("AUCTION_END"):
            AUCTION_END, END_SOURCE = detect_auction_end()
    if isinstance(environment.runner, WorkerRunner):
        return
    lead = AUCTION_END - time.time()
    print("\n[loadtest] configuration")
    print(f"  target            ws://{GATEWAY_ADDR}/ws (reachable={ok})")
    if not ok:
        print(f"  WARNING           target not yet reachable ({err}); users will retry connecting.")
    print(f"  auction end       {datetime.fromtimestamp(AUCTION_END).isoformat(timespec='seconds')} "
          f"({lead:.0f}s from now; source: {END_SOURCE})")
    print(f"  request cap       {MAX_REQUESTS} bids ({SPIKE_RESERVE} reserved for the final {SPIKE_S:.0f}s)")
    print(f"  users             {BASE_USERS} normal -> {SPIKE_USERS} for the spike; processes={PROCESSES}")
    print(f"  bid mix           {1 - LOW_BID_SHARE - DUP_BID_SHARE:.0%} competitive, "
          f"{LOW_BID_SHARE:.0%} below minimum, {DUP_BID_SHARE:.0%} duplicate")
    if END_SOURCE.startswith("FALLBACK"):
        print("  WARNING: could not find the auction end; set AUCTION_END (see the docstring).")
    print(flush=True)


@events.quitting.add_listener
def _on_quitting(environment, **kw):
    with S.lock:
        sent = S.sent
        has_buckets = bool(S.buckets)
        broadcasts = S.broadcasts
        connect_ok = S.connect_ok
        connect_fail = S.connect_fail
        price = S.price
        outcomes = dict(S.outcomes)

    if sent == 0 and not has_buckets:
        return  # master process in --processes mode: nothing recorded here

    rows = rows_from_buckets()
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"timeline_{os.getpid()}.csv")
    write_csv(path, rows)

    print(f"\n[loadtest pid {os.getpid()}] bids sent={sent}  broadcasts received={broadcasts}  "
          f"connects ok/failed={connect_ok}/{connect_fail}  highest price seen={price}")
    print("outcomes (kind -> result: count):")
    for (kind, outcome), n in sorted(outcomes.items(), key=lambda kv: -kv[1]):
        print(f"  {kind:12} -> {outcome:26} {n}")
    print(f"timeline written to {path}")
    if PROCESSES == 1:
        summarize(rows, "SUMMARY")


def merge(paths):
    merged = {}
    for p in paths:
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                t = int(r["t"])
                m = merged.setdefault(t, {
                    "t": t, "rel_to_end_s": float(r["rel_to_end_s"]), "phase": r["phase"],
                    "sent": 0, "responses": 0, "accepted": 0, "rejected": 0, "timeouts": 0,
                    "lat_sum_ms": 0.0, "lat_p50_ms": 0.0, "lat_p95_ms": 0.0,
                    "lat_p99_ms": 0.0, "lat_max_ms": 0.0})
                for k in ("sent", "responses", "accepted", "rejected", "timeouts"):
                    m[k] += int(r[k])
                m["lat_sum_ms"] += float(r["lat_sum_ms"])
                # percentiles cannot be merged exactly; the worst process is a safe upper bound
                for k in ("lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_max_ms"):
                    m[k] = max(m[k], float(r[k]))
    rows = [merged[t] for t in sorted(merged)]
    out = os.path.join(os.path.dirname(paths[0]) or ".", "timeline_merged.csv")
    write_csv(out, rows)
    print(f"merged {len(paths)} timelines -> {out}")
    summarize(rows, "SUMMARY (all processes; latency percentiles are per-process worst case)")


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "merge":
        files = []
        for a in sys.argv[2:]:
            files.extend(sorted(glob.glob(a)))
        files = [f for f in files if not f.endswith("timeline_merged.csv")]
        if not files:
            sys.exit("no timeline CSV files found")
        merge(files)
    else:
        print(__doc__)