#!/usr/bin/env python3
"""stock — track stock prices live for the pages in this folder.

  stock status                          which companies are tracked right now, last price, last update
  stock add SYM [SYM...] [--no-start]   add companies (loads ~4 weeks of history) and start tracking them
  stock remove SYM [SYM...]             stop and drop from the list (stored data is kept in store/)
  stock start [SYM...] [--every SEC]    start tracking all companies, or only the named ones
  stock stop [SYM...]                   stop tracking all companies, or only the named ones
  stock alarm SYM [ALARM...]            add alarms, e.g. '>=1095:sell' '<=1080:COST dropped below 1080'
              [--rm PRICE] [--clear]    remove the alarm(s) at PRICE, or all of the company's alarms
  stock open [SYM]                      open the page in the browser
  stock markers SYM [--rm T]           list the notes pinned on the chart (add them by clicking it)
  stock ranges [add|rm RANGE...]       the page's range buttons (and ?r= values): 2h, 90m, 3d, 2W, 3mo, Day
  stock config [--every SEC] [--days N] [--budget REQ_PER_HOUR]   show or change the defaults

How it works: one tracker process polls every tracked company in a single Yahoo request
(up to 20 companies per request), so adding companies doesn't multiply the request rate.
--every is the seconds between price checks; without it the default (stock config) is used.
The rate is kept under the budget (requests per hour): if a choice would exceed it, the
intervals are stretched and you are told.

When tracking starts, the gap since the last stored price is filled from Yahoo with the
finest bars it still serves for that age: 1-minute for the last 29 days, 5-minute up to
59 days back, hourly up to ~2 years, daily beyond. Live ticks are finer than any of these.

Files: stocks.json (the list and settings), store/SYM/DAY.jsonl (all prices, per New York
trading day), and the *.js files the page reads (data-, hist-, quote-SYM.js, symbols.js,
market.js). tracker.log has the tracker's messages.

While the tracker runs, the pages can add and remove alarms through a small web service on
127.0.0.1 (port in stock config). It only accepts requests carrying the random code the
tracker writes to api.js, which only your own local pages can read.
"""
import argparse, json, os, re, secrets, signal, subprocess, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.realpath(__file__))
CFG = os.path.join(HERE, "stocks.json")
STORE = os.path.join(HERE, "store")
PIDF = os.path.join(HERE, "tracker.pid")
LOG = os.path.join(HERE, "tracker.log")
SAY = os.path.expanduser("~/.claude/bin/claude-done")
NY = ZoneInfo("America/New_York")
YAHOO = "https://query1.finance.yahoo.com"
SPARK_MAX = 20                      # companies per spark request
DEFAULTS = {"every": 5, "days": 28, "budget": 1800, "port": 8765,
            "ranges": ["5m", "15m", "20m", "30m", "45m", "1h", "Day", "1W", "4W"]}
CFG_LOCK = threading.Lock()
MIN_EVERY = 2
# finest bars Yahoo serves by age: (interval, reaches back N days, at most N days per request)
GRAINS = [("1m", 29, 7), ("5m", 59, 59), ("60m", 729, 729), ("1d", 36500, 3650)]


# ---------------------------------------------------------------- requests, rate limited

class Bucket:
    """Token bucket: `per_hour` requests on average, short bursts allowed (gap fills)."""
    def __init__(self, per_hour):
        self.rate, self.cap = per_hour / 3600, max(10, per_hour / 12)
        self.tokens, self.t = self.cap, time.time()

    def take(self, n=1):
        while True:
            now = time.time()
            self.tokens = min(self.cap, self.tokens + (now - self.t) * self.rate)
            self.t = now
            if self.tokens >= n:
                self.tokens -= n
                return
            time.sleep((n - self.tokens) / self.rate)


BUCKET = Bucket(DEFAULTS["budget"])


def get(path):
    BUCKET.take()
    req = urllib.request.Request(YAHOO + path, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def chart(sym, query):
    return get(f"/v8/finance/chart/{sym}?{query}")["chart"]["result"][0]


def spark(syms, query="range=1d&interval=1d"):
    """{SYM: {...}} for up to SPARK_MAX symbols in one request."""
    return get(f"/v8/finance/spark?symbols={','.join(syms)}&{query}")


def bars(res):
    return [[t * 1000, round(c, 4)] for t, c in zip(res.get("timestamp") or [],
                                                     res["indicators"]["quote"][0]["close"]) if c]


def est_per_hour(everies):
    """Requests per hour when companies with these intervals are polled together
    (checks are aligned to multiples of the interval, so they coincide)."""
    n = 0
    for t in range(3600):
        due = sum(1 for e in everies if t % e == 0)
        n += -(-due // SPARK_MAX)
    return n


# ---------------------------------------------------------------- config

def load_cfg():
    if not os.path.exists(CFG):
        return {"defaults": dict(DEFAULTS), "symbols": []}
    with open(CFG) as f:
        c = json.load(f)
    c["defaults"] = {**DEFAULTS, **c.get("defaults", {})}
    return c


def save_cfg(c):
    wjson(CFG, c, indent=1)
    write_symbols(c)
    write_settings(c)


def write_settings(c):
    d = c["defaults"]
    wjs("settings.js", "onSettings", {"ranges": d["ranges"], "days": d["days"]})


RANGE_UNITS = {"m": ("m", 1), "min": ("m", 1), "h": ("h", 60), "hr": ("h", 60), "d": ("d", 1440),
               "day": ("d", 1440), "w": ("W", 10080), "wk": ("W", 10080), "mo": ("mo", 43200),
               "mon": ("mo", 43200), "month": ("mo", 43200),
               "minute": ("m", 1), "hour": ("h", 60), "week": ("W", 10080)}


def parse_range(spec):
    """'2h' -> ('2h', 120 minutes). 'Day' is today's whole session."""
    s = str(spec).strip()
    if s.lower() == "day":
        return "Day", 390
    m = re.fullmatch(r"(\d+)\s*([a-zA-Z]+?)s?", s)
    unit = RANGE_UNITS.get(m.group(2).lower()) if m else None
    if not unit or not 1 <= int(m.group(1)) <= 1000:
        raise ValueError(f"not a range: {spec!r} (use e.g. 90m, 2h, Day, 3d, 2W, 3mo)")
    n = int(m.group(1))
    if n * unit[1] > 24 * 43200:
        raise ValueError(f"{spec}: at most 24 months")
    return f"{n}{unit[0]}", n * unit[1]


def range_days(label):
    """Days of history a multi-day range needs (0 for intraday ranges)."""
    lab, minutes = parse_range(label)
    return 0 if minutes < 1440 or lab == "Day" else -(-minutes // 1440)


def edit_settings(add=(), remove=(), days=None):
    """Change the range list and history length; returns (settings, note)."""
    with CFG_LOCK:
        c = load_cfg()
        d = c["defaults"]
        ranges = {parse_range(r)[0]: parse_range(r)[1] for r in d["ranges"]}
        for r in remove:
            ranges.pop(parse_range(r)[0], None)
        for r in add:
            lab, minutes = parse_range(r)
            ranges[lab] = minutes
        if not ranges:
            raise ValueError("keep at least one range")
        d["ranges"] = [lab for lab, _ in sorted(ranges.items(), key=lambda x: x[1])]
        note, old_days = "", d["days"]
        if days is not None:
            if not 1 <= int(days) <= 730:
                raise ValueError("history must be 1 to 730 days")
            d["days"] = int(days)
        need = max(range_days(r) for r in d["ranges"])
        if need > d["days"]:
            note = f"history raised from {d['days']} to {need} days to fit the longest range"
            d["days"] = need
        save_cfg(c)
    if d["days"] > old_days:
        note = (note + "; " if note else "") + f"older prices back to {d['days']} days are being loaded"
    return {"ranges": d["ranges"], "days": d["days"]}, note


def entry(c, sym):
    return next((e for e in c["symbols"] if e["symbol"] == sym), None)


def wjson(path, obj, indent=None):
    with open(path + ".tmp", "w") as f:
        json.dump(obj, f, indent=indent, separators=None if indent else (",", ":"))
    os.replace(path + ".tmp", path)


def wjs(name, fn, obj):
    """A .js file the page loads with a <script> tag: works over file://, unlike fetch()."""
    path = os.path.join(HERE, name)
    with open(path + ".tmp", "w") as f:
        f.write(f"window.{fn}(" + json.dumps(obj, separators=(",", ":")) + ");\n")
    os.replace(path + ".tmp", path)


def write_symbols(c):
    lst = [{k: e.get(k) for k in ("symbol", "tracking", "every", "since", "markers")} for e in c["symbols"]]
    with open(os.path.join(HERE, "symbols.js"), "w") as f:
        f.write("window.SYMBOLS = " + json.dumps(lst) + ";\n")


def now_ms():
    return int(time.time() * 1000)


# ---------------------------------------------------------------- store

def ny_day(ms):
    return datetime.fromtimestamp(ms / 1000, NY).strftime("%Y-%m-%d")


def sdir(sym):
    p = os.path.join(STORE, sym)
    os.makedirs(p, exist_ok=True)
    return p


def day_files(sym):
    return sorted(f[:-6] for f in os.listdir(sdir(sym)) if re.fullmatch(r"\d{4}-\d\d-\d\d\.jsonl", f))


def load_day(sym, day):
    p = os.path.join(sdir(sym), day + ".jsonl")
    if not os.path.exists(p):
        return []
    with open(p) as f:
        return [json.loads(l) for l in f if l.strip()]


def append(sym, pts):
    by_day = {}
    for p in pts:
        by_day.setdefault(ny_day(p[0]), []).append(p)
    for day, ps in by_day.items():
        with open(os.path.join(sdir(sym), day + ".jsonl"), "a") as f:
            f.writelines(json.dumps(p) + "\n" for p in ps)


def merge_into(sym, pts):
    """Store prices that may be older than what a day file holds: merge and rewrite."""
    by_day = {}
    for p in pts:
        by_day.setdefault(ny_day(p[0]), []).append(p)
    for day, ps in by_day.items():
        merged = {t: v for t, v in load_day(sym, day)}
        merged.update({t: v for t, v in ps})
        path = os.path.join(sdir(sym), day + ".jsonl")
        with open(path + ".tmp", "w") as f:
            f.writelines(json.dumps([t, merged[t]]) + "\n" for t in sorted(merged))
        os.replace(path + ".tmp", path)


def first_point(sym):
    for day in day_files(sym):
        pts = load_day(sym, day)
        if pts:
            return pts[0]
    return None


def last_point(sym):
    days = day_files(sym)
    for day in reversed(days):
        pts = load_day(sym, day)
        if pts:
            return pts[-1]
    return None


def load_meta(sym):
    p = os.path.join(sdir(sym), "meta.json")
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return {}


def save_meta(sym, m):
    wjson(os.path.join(sdir(sym), "meta.json"), m)


def load_hits(sym, day):
    p = os.path.join(sdir(sym), f"hits-{day}.json")
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return {}


def akey(a):
    return f"{a['op']}{float(a['price'])}"


def fill(sym, frm_ms, to_ms, log=print):
    """Prices for (frm_ms, to_ms] from Yahoo, as fine as Yahoo serves for each age."""
    now = time.time()
    out, upper, requests = {}, to_ms / 1000, 0
    for iv, reach_days, chunk_days in GRAINS:
        lower = max(frm_ms / 1000, now - reach_days * 86400)
        a = lower
        while upper - a >= 60:
            e = min(upper, a + chunk_days * 86400)
            try:
                res = chart(sym, f"interval={iv}&period1={int(a)}&period2={int(e) + 60}")
                requests += 1
                for t, c in bars(res):
                    if frm_ms < t <= to_ms:
                        out[t] = c
            except Exception as ex:
                log(f"{sym}: no {iv} data for {ny_day(a * 1000)}..{ny_day(e * 1000)} ({ex})")
            a = e
        upper = min(upper, lower)
        if upper <= frm_ms / 1000:
            break
    return sorted([t, c] for t, c in out.items()), requests


def refresh_daily(sym, days, log=print):
    """Official open and close of each session (Yahoo's daily bars) into meta.json."""
    meta = load_meta(sym)
    try:
        res = chart(sym, f"interval=1d&range={'3mo' if days <= 80 else '1y' if days <= 360 else '2y'}")
        q = res["indicators"]["quote"][0]
        daily = meta.get("daily", {})
        for t, o, c in zip(res.get("timestamp") or [], q["open"], q["close"]):
            if o and c:
                daily[ny_day(t * 1000)] = [round(o, 4), round(c, 4)]
        meta["daily"] = daily
        save_meta(sym, meta)
    except Exception as ex:
        log(f"{sym}: could not read daily open/close ({ex})")
    return meta


def meta_from_yahoo(sym):
    m = chart(sym, "interval=1d&range=5d")["meta"]
    meta = load_meta(sym)
    meta.update({"name": m.get("longName") or m.get("shortName"), "currency": m.get("currency"),
                 "prevClose": m.get("chartPreviousClose")})
    save_meta(sym, meta)
    return meta


def catch_up(sym, days, log=print):
    """Fill the gap from the last stored price (or `days` back) to now, and load older
    prices when the history should reach further back than the store does."""
    want_from = now_ms() - days * 86400000
    first, last = first_point(sym), last_point(sym)
    if first and first[0] - want_from > 86400000:
        meta = load_meta(sym)
        tried = meta.get("history_from")           # don't ask again for what Yahoo didn't have
        if tried is None or want_from < tried - 86400000:
            pts, n = fill(sym, want_from, first[0] - 1, log)
            merge_into(sym, pts)
            meta["history_from"] = want_from
            save_meta(sym, meta)
            log(f"{sym}: loaded {len(pts)} older prices back to {ny_day(want_from)} with {n} request(s)")
    frm = last[0] if last else want_from
    pts, n = fill(sym, frm, now_ms(), log)
    append(sym, pts)
    if pts:
        log(f"{sym}: filled {len(pts)} prices from {ny_day(frm)} with {n} request(s)")
    return pts


def extend_all(c, log=print, tracks=None):
    """After the history length grew: load older prices for every company, rewrite its history."""
    for e in c["symbols"]:
        sym = e["symbol"]
        catch_up(sym, c["defaults"]["days"], log)
        if tracks and sym in tracks:
            tracks[sym].hist_version = write_hist(sym, tracks[sym].session, c["defaults"]["days"])
        else:
            write_static(sym, c, "stopped")


# ---------------------------------------------------------------- page outputs

def session_of(sym):
    days = day_files(sym)
    return days[-1] if days else ny_day(now_ms())


def write_hist(sym, session, days):
    """Days before the session, at most 1 point per minute (keeps the file small)."""
    since = ny_day(now_ms() - days * 86400000)
    out = []
    for d in day_files(sym):
        if since <= d < session:
            last_min = None
            for t, p in load_day(sym, d):
                m = t // 60000
                if m == last_min:
                    out[-1] = [t, p]
                else:
                    out.append([t, p])
                last_min = m
    version = f"{session}-{len(out)}-{out[-1][0] if out else 0}"
    wjs(f"hist-{sym}.js", "onHist", {"symbol": sym, "version": version, "bars": out})
    return version


def prev_close(meta, session):
    """Close of the session before `session`, from the daily bars. Yahoo's quick quote
    (chartPreviousClose) is only the fallback: it has been seen off (COST, $30 out on
    2026-11-24 while the daily bars and the minute prices agreed)."""
    daily = meta.get("daily") or {}
    before = [d for d in daily if d < session]
    return daily[max(before)][1] if before else meta.get("prevClose")


def write_data(sym, *, pts, session, meta, alarms, state, every, hist_version, err=None, days=DEFAULTS["days"]):
    prices = [p for _, p in pts]
    pc = prev_close(meta, session)
    obj = {"symbol": sym, "name": meta.get("name"), "currency": meta.get("currency"),
           "prevClose": pc, "quotePrevClose": meta.get("prevClose"), "session": session,
           "high": max(prices) if prices else None, "low": min(prices) if prices else None,
           "points": pts, "alarms": alarms, "state": state, "every": every,
           "updated": now_ms(), "error": err, "histVersion": hist_version,
           # official open/close per session, for the days the page can show
           "daily": {d: v for d, v in (meta.get("daily") or {}).items() if d >= ny_day(now_ms() - (days + 5) * 86400000)}}
    wjs(f"data-{sym}.js", "onFeed", obj)
    last = prices[-1] if prices else None
    wjs(f"quote-{sym}.js", "onQuote", {
        "symbol": sym, "price": last, "prevClose": pc, "state": state,
        "every": every, "updated": obj["updated"],
        "alarmHit": last is not None and any(hit(a, last) for a in alarms)})


def write_static(sym, c, state):
    """Outputs for a company the tracker isn't polling (stopped, just added)."""
    e = entry(c, sym)
    session = session_of(sym)
    hv = write_hist(sym, session, c["defaults"]["days"])
    hits = load_hits(sym, session)
    alarms = [{**a, "armed": False, "hits": hits.get(akey(a), [])} for a in e.get("alarms", [])]
    write_data(sym, days=c["defaults"]["days"], pts=load_day(sym, session), session=session, meta=load_meta(sym), alarms=alarms,
               state=state, every=e.get("every"), hist_version=hv)


def hit(a, p):
    return p >= a["price"] if a["op"] == ">=" else p <= a["price"]


def rearm(a, p):
    return p < a["price"] - 1 if a["op"] == ">=" else p > a["price"] + 1


# ---------------------------------------------------------------- the tracker process

def tracker_pid():
    try:
        pid = int(open(PIDF).read())
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            if b"stock.py" in f.read():
                return pid
    except (OSError, ValueError):
        pass
    return None


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


class Track:
    def __init__(self, sym, c):
        self.sym = sym
        days = c["defaults"]["days"]
        try:
            self.meta = meta_from_yahoo(sym)
        except Exception as ex:
            log(f"{sym}: could not read company info ({ex})")
            self.meta = load_meta(sym)
        catch_up(sym, days, log)
        self.session = session_of(sym)
        self.pts = load_day(sym, self.session)
        self.hist_version = write_hist(sym, self.session, days)
        self.days = days
        self.meta = refresh_daily(sym, days, log)
        self.err, self.next_due = None, 0
        self.configure(entry(c, sym))

    def configure(self, e):
        self.every = max(MIN_EVERY, int(e.get("every") or DEFAULTS["every"]))
        hits = load_hits(self.sym, self.session)
        cur = self.pts[-1][1] if self.pts else None
        old = {akey(a): a for a in getattr(self, "alarms", [])}
        self.alarms = []
        for a in e.get("alarms", []):
            k = akey(a)
            armed = old[k]["armed"] if k in old else (cur is None or not hit(a, cur))
            self.alarms.append({**a, "price": float(a["price"]), "armed": armed, "hits": hits.get(k, [])})

    def tick(self, t, p, prev_close, days):
        day = ny_day(t)
        if day != self.session:          # a new trading day
            self.session, self.pts = day, []
            self.hist_version = write_hist(self.sym, day, days)
            for a in self.alarms:
                a["hits"] = []
        if prev_close and prev_close != self.meta.get("prevClose"):
            self.meta["prevClose"] = prev_close
            save_meta(self.sym, self.meta)
        if self.pts and t <= self.pts[-1][0]:
            return
        self.pts.append([t, p])
        append(self.sym, [[t, p]])
        fired = False
        for a in self.alarms:
            if a["armed"] and hit(a, p):
                a["armed"] = False
                a["hits"].append([t, p])
                fired = True
                subprocess.Popen([SAY, a["say"]], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                log(f"ALARM {self.sym} {a['op']}{a['price']} at {p}")
            elif not a["armed"] and rearm(a, p):
                a["armed"] = True
        if fired:
            wjson(os.path.join(sdir(self.sym), f"hits-{self.session}.json"),
                  {akey(a): a["hits"] for a in self.alarms})

    def write(self, state):
        write_data(self.sym, days=self.days, pts=self.pts, session=self.session, meta=self.meta, alarms=self.alarms,
                   state=state, every=self.every, hist_version=self.hist_version, err=self.err)


class Api(BaseHTTPRequestHandler):
    """POST /alarm {token, symbol, op, price, say}   POST /alarm/remove {token, symbol, op, price}"""
    token = None

    def reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        for k, v in (("Access-Control-Allow-Origin", "*"), ("Access-Control-Allow-Methods", "POST"),
                     ("Access-Control-Allow-Headers", "Content-Type"), ("Access-Control-Allow-Private-Network", "true")):
            self.send_header(k, v)
        self.end_headers()

    def do_POST(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            if n > 4096:
                return self.reply(413, {"error": "too large"})
            req = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(req, dict) or not secrets.compare_digest(str(req.get("token", "")), Api.token):
                return self.reply(403, {"error": "bad token: reload the page"})
            sym = str(req.get("symbol", "")).upper()
            if self.path == "/alarm":
                a = make_alarm(sym, req.get("op"), req.get("price"), req.get("say"))
                alarms = edit_alarms(sym, add=[a])
                log(f"{sym}: alarm {a['op']}{a['price']} \"{a['say']}\" set from the page")
            elif self.path == "/marker":
                ms = edit_markers(sym, add=req)
                return self.reply(200, {"ok": True, "markers": ms})
            elif self.path == "/marker/remove":
                ms = edit_markers(sym, remove_t=req.get("t"))
                return self.reply(200, {"ok": True, "markers": ms})
            elif self.path == "/settings":
                settings, note = edit_settings(req.get("add") or [], req.get("remove") or [], req.get("days"))
                log(f"ranges set from the page: {' '.join(settings['ranges'])}, {settings['days']} days")
                return self.reply(200, {"ok": True, **settings, "note": note})
            elif self.path == "/alarm/remove":
                key = f"{req.get('op')}{float(req.get('price'))}"
                alarms = edit_alarms(sym, remove=[key])
                log(f"{sym}: alarm {key} removed from the page")
            else:
                return self.reply(404, {"error": "unknown"})
            self.reply(200, {"ok": True, "alarms": alarms})
        except (ValueError, TypeError, KeyError) as ex:
            self.reply(400, {"error": str(ex)})

    def log_message(self, *a):
        pass


def start_api(port):
    Api.token = secrets.token_urlsafe(18)
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", port), Api)
    except OSError as ex:
        log(f"alarm editing from the page unavailable: port {port} busy ({ex})")
        return
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with open(os.path.join(HERE, "api.js"), "w") as f:
        f.write("window.API = " + json.dumps({"url": f"http://127.0.0.1:{port}", "token": Api.token}) + ";\n")
    log(f"pages can edit alarms via 127.0.0.1:{port}")


def stop_api():
    with open(os.path.join(HERE, "api.js"), "w") as f:
        f.write("window.API = null;\n")


def market_period(sym):
    """(start, end) of the current or latest regular session, unix seconds, from Yahoo.
    Before the open Yahoo may still report yesterday's session: on a weekday before 16:00
    New York time, assume today's regular 9:30-16:00 session then."""
    d = spark([sym], "range=1d&interval=1m")[sym]
    start, end = d["start"], d["end"]
    ny = datetime.now(NY)
    if end < time.time() and ny.weekday() < 5 and ny.hour < 16:
        start = ny.replace(hour=9, minute=30, second=0, microsecond=0).timestamp()
        end = ny.replace(hour=16, minute=0, second=0, microsecond=0).timestamp()
    return start, end


def run():
    global BUCKET
    if tracker_pid():
        log("tracker already running")
        return
    with open(PIDF, "w") as f:
        f.write(str(os.getpid()))
    tracks, c, mtime = {}, None, 0

    def shutdown(state):
        stop_api()
        for tr in tracks.values():
            tr.write(state)
        try:
            os.remove(PIDF)
        except OSError:
            pass

    def on_term(*_):
        log("stopped")
        shutdown("stopped")
        sys.exit(0)
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    period, period_at, days_seen = None, 0, None
    log("tracker started")
    start_api(load_cfg()["defaults"]["port"])
    while True:
        if os.path.getmtime(CFG) != mtime:
            with CFG_LOCK:
                mtime = os.path.getmtime(CFG)
                c = load_cfg()
            BUCKET = Bucket(c["defaults"]["budget"]) if BUCKET.rate * 3600 != c["defaults"]["budget"] else BUCKET
            want = {e["symbol"]: e for e in c["symbols"] if e.get("tracking")}
            for sym in [s for s in tracks if s not in want]:
                tracks.pop(sym).write("stopped")
                log(f"{sym}: tracking stopped")
            if days_seen is not None and c["defaults"]["days"] > days_seen:
                log(f"history length now {c['defaults']['days']} days: loading older prices")
                extend_all(c, log, tracks)
            elif days_seen is not None and c["defaults"]["days"] != days_seen:
                for tr in tracks.values():
                    tr.hist_version = write_hist(tr.sym, tr.session, c["defaults"]["days"])
            days_seen = c["defaults"]["days"]
            for tr in tracks.values():
                tr.days = days_seen
            for sym, e in want.items():
                if sym in tracks:
                    tracks[sym].configure(e)
                else:
                    log(f"{sym}: tracking started, every {e.get('every')}s")
                    tracks[sym] = Track(sym, c)
                    tracks[sym].write("tracking")
            if not tracks:
                log("nothing to track, tracker exiting")
                shutdown("stopped")
                return

        now = time.time()
        if not period or now - period_at > 1800:
            try:
                period, period_at = market_period(next(iter(tracks))), now
                wjs("market.js", "onMarket", {"start": period[0], "end": period[1]})
                if now > period[0]:                    # today's open is known once trading started
                    for tr in tracks.values():
                        tr.meta = refresh_daily(tr.sym, tr.days, log)
            except Exception as ex:
                log(f"could not read market hours ({ex})")
                period, period_at = period or (0, 1e12), now
        start, end = period
        if now > end + 300:
            log("market closed, tracker exiting (history is filled up to the close)")
            for tr in tracks.values():                 # keep the official close
                tr.meta = refresh_daily(tr.sym, tr.days, log)
            shutdown("closed")
            return
        if now < start - 60:
            for tr in tracks.values():
                tr.write("waiting")
            time.sleep(min(30, start - 60 - now))
            period = None if start - now < 120 else period
            continue

        due = [tr for tr in tracks.values() if now >= tr.next_due]
        for i in range(0, len(due), SPARK_MAX):
            group = due[i:i + SPARK_MAX]
            try:
                res = spark([tr.sym for tr in group])
                for tr in group:
                    d = res.get(tr.sym) or {}
                    if d.get("close") and d.get("timestamp"):
                        tr.tick(d["timestamp"][-1] * 1000, round(d["close"][-1], 4),
                                d.get("chartPreviousClose"), c["defaults"]["days"])
                        tr.err = None
                    else:
                        tr.err = "no price in response"
            except Exception as ex:
                for tr in group:
                    tr.err = str(ex)
                log(f"poll failed: {ex}")
                for tr in group:                      # back off a little on errors
                    tr.next_due = now + 30
            for tr in group:
                tr.write("tracking")
                if tr.next_due <= now:
                    tr.next_due = (int(now) // tr.every + 1) * tr.every   # aligned, so polls coincide
        nxt = min(tr.next_due for tr in tracks.values())
        time.sleep(max(0.2, min(1.0, nxt - time.time())))


def ensure_tracker():
    pid = tracker_pid()
    if pid:
        return pid
    with open(LOG, "a") as out:
        p = subprocess.Popen([sys.executable, "-I", os.path.realpath(__file__), "_run"], cwd=HERE,
                             stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                             start_new_session=True)
    return p.pid


def stop_tracker():
    pid = tracker_pid()
    if not pid:
        return False
    os.kill(pid, signal.SIGTERM)
    for _ in range(50):
        if not tracker_pid():
            break
        time.sleep(0.1)
    return True


# ---------------------------------------------------------------- commands

def fit_budget(c):
    """Stretch intervals of tracked companies until the estimated rate fits the budget."""
    tracked = [e for e in c["symbols"] if e.get("tracking")]
    budget = c["defaults"]["budget"]
    if not tracked:
        return
    est = est_per_hour([e["every"] for e in tracked])
    if est <= budget:
        return
    before = {e["symbol"]: e["every"] for e in tracked}
    while est_per_hour([e["every"] for e in tracked]) > budget:
        for e in tracked:
            e["every"] = e["every"] + max(1, e["every"] // 4)
    print(f"! {est} requests/hour would exceed the budget of {budget}; intervals stretched:")
    for e in tracked:
        if e["every"] != before[e["symbol"]]:
            print(f"    {e['symbol']}: every {before[e['symbol']]}s -> {e['every']}s")


def resolve(c, syms):
    if not syms:
        return [e["symbol"] for e in c["symbols"]]
    known = {e["symbol"] for e in c["symbols"]}
    bad = [s for s in syms if s not in known]
    if bad:
        sys.exit(f"not in the list: {' '.join(bad)} (add with: stock add {' '.join(bad)})")
    return syms


def cmd_start(a):
    c = load_cfg()
    syms = resolve(c, [s.upper() for s in a.symbols])
    every = a.every or c["defaults"]["every"]
    if every < MIN_EVERY:
        sys.exit(f"--every must be at least {MIN_EVERY} seconds")
    for e in c["symbols"]:
        if e["symbol"] in syms:
            if not e.get("tracking"):
                e["since"] = now_ms()
            e["tracking"], e["every"] = True, every
    fit_budget(c)
    save_cfg(c)
    pid = tracker_pid()
    ensure_tracker()
    print(("tracker already running, it picks the change up now" if pid else "tracker started") +
          " — gaps are filled first, then prices every few seconds")
    time.sleep(4)
    cmd_status(a)


def cmd_stop(a):
    c = load_cfg()
    syms = resolve(c, [s.upper() for s in a.symbols])
    for e in c["symbols"]:
        if e["symbol"] in syms and e.get("tracking"):
            e["tracking"], e["since"] = False, now_ms()
    save_cfg(c)
    if not any(e.get("tracking") for e in c["symbols"]):
        stop_tracker()
    for s in syms:            # make the page show it even if the tracker isn't running
        if os.path.isdir(os.path.join(STORE, s)):
            write_static(s, c, "stopped")
    cmd_status(a)


def cmd_add(a):
    c = load_cfg()
    added = []
    for sym in [s.upper() for s in a.symbols]:
        if entry(c, sym):
            print(f"{sym}: already in the list")
            continue
        try:
            meta = meta_from_yahoo(sym)
        except Exception as ex:
            print(f"{sym}: not found on Yahoo Finance ({ex})")
            continue
        print(f"{sym}: {meta.get('name')} — loading {c['defaults']['days']} days of history...")
        catch_up(sym, c["defaults"]["days"], lambda m: print("   ", m))
        refresh_daily(sym, c["defaults"]["days"], lambda m: print("   ", m))
        c["symbols"].append({"symbol": sym, "tracking": False, "every": c["defaults"]["every"],
                             "since": now_ms(), "alarms": []})
        save_cfg(c)
        write_static(sym, c, "stopped")
        added.append(sym)
    if added and not a.no_start:
        cmd_start(argparse.Namespace(symbols=added, every=a.every))
    elif added:
        cmd_status(a)


def cmd_remove(a):
    c = load_cfg()
    syms = resolve(c, [s.upper() for s in a.symbols])
    c["symbols"] = [e for e in c["symbols"] if e["symbol"] not in syms]
    save_cfg(c)
    if not any(e.get("tracking") for e in c["symbols"]):
        stop_tracker()
    for s in syms:
        for f in (f"data-{s}.js", f"hist-{s}.js", f"quote-{s}.js"):
            try:
                os.remove(os.path.join(HERE, f))
            except OSError:
                pass
    print(f"removed {' '.join(syms)} (its prices stay in store/; stock add brings it back)")


def make_alarm(sym, op, price, words=None):
    if op not in (">=", "<="):
        raise ValueError("the alarm needs >= (rises to) or <= (falls to)")
    price = round(float(price), 4)
    if not 0 < price < 1e7:
        raise ValueError("price out of range")
    words = re.sub(r"[\x00-\x1f]", " ", (words or "").strip())[:100]
    return {"op": op, "price": price,
            "say": words or f"{sym} {'reached' if op == '>=' else 'dropped below'} {price:g}"}


def edit_alarms(sym, add=(), remove=(), clear=False):
    """Add alarm dicts, remove by key (">=1095.0") or price; returns the company's alarms."""
    with CFG_LOCK:
        c = load_cfg()
        e = entry(c, sym)
        if not e:
            raise ValueError(f"{sym} is not in the list")
        if clear:
            e["alarms"] = []
        for r in remove:
            e["alarms"] = [x for x in e["alarms"] if akey(x) != r and str(float(x["price"])) != str(r)]
        for a in add:
            e["alarms"] = [x for x in e["alarms"] if akey(x) != akey(a)] + [a]
        e["alarms"].sort(key=lambda x: -x["price"])
        save_cfg(c)
    if not e.get("tracking"):
        write_static(sym, c, "stopped")
    return e["alarms"]


def edit_markers(sym, add=None, remove_t=None):
    """Markers are notes pinned to the chart ({t, price, label}), e.g. where you bought."""
    with CFG_LOCK:
        c = load_cfg()
        e = entry(c, sym)
        if not e:
            raise ValueError(f"{sym} is not in the list")
        ms = e.setdefault("markers", [])
        if remove_t is not None:
            ms[:] = [m for m in ms if m["t"] != int(remove_t)]
        if add:
            t, price = int(add["t"]), round(float(add["price"]), 4)
            if not 0 < price < 1e7 or not 0 < t < 4e12:
                raise ValueError("bad marker")
            label = re.sub(r"[\x00-\x1f]", " ", str(add.get("label") or "").strip())[:80] or "marker"
            ms[:] = [m for m in ms if m["t"] != t] + [{"t": t, "price": price, "label": label}]
        ms.sort(key=lambda m: m["t"])
        save_cfg(c)
    return ms


def cmd_markers(a):
    sym = a.symbol.upper()
    try:
        ms = edit_markers(sym, remove_t=a.rm)
    except ValueError as ex:
        sys.exit(str(ex))
    for m in ms:
        print(f"  {datetime.fromtimestamp(m['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')}  {m['price']:>10,.2f}  {m['label']}"
              f"   (--rm {m['t']})")
    if not ms:
        print(f"  {sym}: no markers (click the chart to add one)")


def cmd_alarm(a):
    sym = a.symbol.upper()
    add = []
    for spec in a.alarms:
        m = re.fullmatch(r"(>=|<=)\s*([0-9.]+)(?::(.*))?", spec)
        if not m:
            sys.exit(f"bad alarm {spec!r}: use '>=PRICE:words' or '<=PRICE:words'")
        add.append(make_alarm(sym, m.group(1), m.group(2), m.group(3)))
    try:
        alarms = edit_alarms(sym, add, [float(a.rm)] if a.rm is not None else [], a.clear)
    except ValueError as ex:
        sys.exit(str(ex))
    e = {"alarms": alarms}
    for x in e["alarms"]:
        print(f"  {sym} {x['op']} {x['price']:g}  says \"{x['say']}\"")
    if not e["alarms"]:
        print(f"  {sym}: no alarms")


def read_js(name):
    try:
        s = open(os.path.join(HERE, name)).read()
        return json.loads(s[s.index("(") + 1: s.rindex(")")])
    except (OSError, ValueError):
        return None


def ago(ms):
    s = max(0, (now_ms() - ms) / 1000)
    return f"{s:.0f}s ago" if s < 90 else f"{s / 60:.0f}m ago" if s < 5400 else f"{s / 3600:.1f}h ago" \
        if s < 172800 else f"{s / 86400:.0f}d ago"


def cmd_status(a):
    c = load_cfg()
    pid = tracker_pid()
    tracked = [e for e in c["symbols"] if e.get("tracking")]
    est = est_per_hour([e["every"] for e in tracked]) if tracked else 0
    print(f"tracker: {'running (pid ' + str(pid) + ')' if pid else 'not running'}"
          f" · {est} requests/hour of a {c['defaults']['budget']} budget")
    m = read_js("market.js")
    if m:
        now = time.time()
        if now < m["start"]:
            print(f"market: opens in {(m['start'] - now) / 60:.0f} min")
        elif now < m["end"]:
            r = int(m["end"] - now)
            print(f"market: open, closes in {r // 3600}h {r % 3600 // 60:02d}m")
        else:
            print("market: closed")
    print(f"\n{'':2}{'SYMBOL':7}{'STATE':30}{'EVERY':>6}{'PRICE':>11}{'TODAY':>9}  {'UPDATED':12}ALARMS")
    for e in c["symbols"]:
        q = read_js(f"quote-{e['symbol']}.js") or {}
        live = pid and e.get("tracking") and q.get("state") == "tracking" and \
            now_ms() - q.get("updated", 0) < max(30000, 4000 * e["every"])
        if live:
            state, dot = "tracking", "●"
        elif e.get("tracking"):
            state, dot = {"closed": "on, market closed", "waiting": "on, waiting for the open"}.get(
                q.get("state"), "on, but tracker not running"), "◐"
        else:
            state, dot = f"stopped {ago(e.get('since') or 0)}", "○"
        p, pc = q.get("price"), q.get("prevClose")
        chg = f"{(p - pc) / pc * 100:+.2f}%" if p and pc else ""
        print(f"{dot} {e['symbol']:7}{state:30}{str(e['every']) + 's':>6}"
              f"{(f'{p:,.2f}' if p else '—'):>11}{chg:>9}  {(ago(q['updated']) if q.get('updated') else '—'):12}"
              f"{len(e.get('alarms', [])) or ''}")
    if not c["symbols"]:
        print("  (no companies yet: stock add SYMBOL)")


def cmd_open(a):
    sym = (a.symbol or "").upper()
    url = f"file://{HERE}/index.html" + (f"?s={sym}" if sym else "")
    subprocess.Popen(["xdg-open", url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(url)


def cmd_ranges(a):
    try:
        if a.action == "add":
            settings, note = edit_settings(add=a.specs)
        elif a.action == "rm":
            settings, note = edit_settings(remove=a.specs)
        else:
            settings, note = edit_settings()
    except ValueError as ex:
        sys.exit(str(ex))
    print("ranges:", "  ".join(settings["ranges"]), f"   (history kept: {settings['days']} days)")
    if note:
        print(note)
    if "being loaded" in note and not tracker_pid():
        extend_all(load_cfg())


def cmd_config(a):
    c = load_cfg()
    old_days = c["defaults"]["days"]
    for k in ("every", "budget", "port"):
        v = getattr(a, k)
        if v is not None:
            c["defaults"][k] = v
    save_cfg(c)
    if a.days is not None:
        try:
            _, note = edit_settings(days=a.days)
        except ValueError as ex:
            sys.exit(str(ex))
        if note:
            print(note)
        c = load_cfg()
        if c["defaults"]["days"] > old_days and not tracker_pid():
            extend_all(c)
    d = c["defaults"]
    print(f"every  {d['every']}s   seconds between price checks when start has no --every\n"
          f"days   {d['days']}     history kept on the page and loaded for new companies\n"
          f"budget {d['budget']}  most Yahoo requests per hour\n"
          f"port   {d['port']}  where the pages send alarm changes (127.0.0.1 only)")


def main():
    p = argparse.ArgumentParser(prog="stock", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog=__doc__.split("\n\n", 1)[1])
    sub = p.add_subparsers(dest="cmd")
    s = sub.add_parser("status"); s.set_defaults(fn=cmd_status)
    s = sub.add_parser("add"); s.add_argument("symbols", nargs="+"); s.add_argument("--no-start", action="store_true")
    s.add_argument("--every", type=int); s.set_defaults(fn=cmd_add)
    s = sub.add_parser("remove"); s.add_argument("symbols", nargs="+"); s.set_defaults(fn=cmd_remove)
    s = sub.add_parser("start"); s.add_argument("symbols", nargs="*"); s.add_argument("--every", type=int)
    s.set_defaults(fn=cmd_start)
    s = sub.add_parser("stop"); s.add_argument("symbols", nargs="*"); s.set_defaults(fn=cmd_stop)
    s = sub.add_parser("alarm"); s.add_argument("symbol"); s.add_argument("alarms", nargs="*")
    s.add_argument("--rm", type=float); s.add_argument("--clear", action="store_true"); s.set_defaults(fn=cmd_alarm)
    s = sub.add_parser("open"); s.add_argument("symbol", nargs="?"); s.set_defaults(fn=cmd_open)
    s = sub.add_parser("markers"); s.add_argument("symbol"); s.add_argument("--rm", type=int)
    s.set_defaults(fn=cmd_markers)
    s = sub.add_parser("ranges"); s.add_argument("action", nargs="?", choices=["add", "rm"])
    s.add_argument("specs", nargs="*"); s.set_defaults(fn=cmd_ranges)
    s = sub.add_parser("config"); s.add_argument("--every", type=int); s.add_argument("--days", type=int)
    s.add_argument("--budget", type=int); s.add_argument("--port", type=int); s.set_defaults(fn=cmd_config)
    s = sub.add_parser("_run"); s.set_defaults(fn=lambda a: run())
    a = p.parse_args()
    if not a.cmd:
        a = p.parse_args(["status"])
    a.fn(a)


if __name__ == "__main__":
    main()
