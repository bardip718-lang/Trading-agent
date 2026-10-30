#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEME DESK LIVE v3: live crypto market-intelligence terminal (RESEARCH ONLY)

This app analyses public market data. It does not trade, simulate trades, hold keys or talk to brokers.
Every number shown comes from a documented API or from prices this app collected itself (labelled).

Run (Pydroid 3): open this file, tap the yellow play button, then open http://127.0.0.1:8765 in Chrome.
Needs:           Python 3.8+ and nothing else (standard library only).
Optional keys:   AI provider key, Solana RPC URL, CoinGecko demo key. Enter them in Settings (stored only
                 on this device in meme_desk_secrets.json) or set DESK_AI_KEY / DESK_SOL_RPC / DESK_CG_KEY.
Pump.fun: it has no official data API. Launches and graduations come from the unofficial third-party PumpPortal stream
(free streams only, switch off in Settings). Not financial advice. Trending, boosted and brand-new tokens are frequently scams.
"""
import calendar, hmac, json, math, os, re, secrets as pysecrets, shutil, signal, socket, statistics
import sys, threading, time, urllib.error, urllib.parse, urllib.request
from collections import OrderedDict, defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "3.1.0"
HOST = os.environ.get("DESK_HOST", "127.0.0.1")
PORT = int(os.environ.get("DESK_PORT", "8765"))
DATA_DIR = os.environ.get("DESK_DATA_DIR", os.getcwd())
DS_BASE = os.environ.get("DESK_DS_BASE", "https://api.dexscreener.com")
GT_BASE_FREE = os.environ.get("DESK_GT_BASE", "https://api.geckoterminal.com/api/v2")
GT_BASE_KEYED = os.environ.get("DESK_CG_BASE", "https://api.coingecko.com/api/v3/onchain")
AI_URL_OVERRIDE = os.environ.get("DESK_AI_URL", "")
DEFAULT_SOL_RPC = "https://api.mainnet-beta.solana.com"
STATE_FILE = os.path.join(DATA_DIR, "meme_desk_state.json")
OLD_STATE_FILE = os.path.join(DATA_DIR, "paper_state.json")     # from v1/v2; only the watchlist is imported, file is never modified
CONFIG_FILE = os.path.join(DATA_DIR, "meme_desk_config.json")
SECRETS_FILE = os.path.join(DATA_DIR, "meme_desk_secrets.json")
TOKEN = pysecrets.token_urlsafe(18)          # per-run token: stops other websites from posting to this local server
BACKOFF = 1.0
W = ("m5", "h1", "h6", "h24")

LOCK = threading.RLock()
STOP = threading.Event()
WAKE = threading.Event()


class ApiError(Exception):
    def __init__(self, msg, fatal=False):
        super().__init__(msg)
        self.fatal = fatal


# ----------------------------------------------------------------------------- settings
DEFAULTS = {
    "refresh_s": 30, "max_tracked": 60, "discovery": True, "pump_stream": True, "min_liq_discovery": 10000.0, "chains": [],
    "min_liq": 25000.0, "min_age_h": 2.0, "whale_usd": 5000.0, "whale_liq_pct": 1.0,
    "trades_per_cycle": 2, "gt_rpm": 10,
    "ai_provider": "anthropic", "ai_model": "claude-sonnet-5-5", "ai_base": "https://api.openai.com/v1", "ai_max_age_min": 30.0,
}
SCHEMA = {  # name: (type, min, max)
    "refresh_s": (float, 15, 600), "max_tracked": (int, 10, 100), "discovery": (bool,), "pump_stream": (bool,),
    "min_liq_discovery": (float, 0, 1e9), "min_liq": (float, 0, 1e9), "min_age_h": (float, 0, 1e5),
    "whale_usd": (float, 100, 1e9), "whale_liq_pct": (float, 0.1, 100), "trades_per_cycle": (int, 0, 10),
    "gt_rpm": (int, 3, 30), "ai_max_age_min": (float, 1, 1440),
}
KEY_RE = re.compile(r"^[a-z0-9\-]{2,30}:[A-Za-z0-9_\-]{10,100}$")
ADDR_RE = re.compile(r"^[A-Za-z0-9_\-]{20,100}$")
SOL_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

S = {"ver": 0, "settings": dict(DEFAULTS), "secrets": {}, "coins": {}, "hist": {}, "watch": [], "wallets": [],
     "feed": [], "warn": [], "agents": {}, "ai": {"coins": {}, "history": [], "busy": False, "error": None, "seq": 0},
     "onchain": {}, "ohlcv": {}, "trades": {}, "overview": None, "launch": {}, "last_fetch": {}, "focus": {},
     "cycle": {"n": 0, "start": 0, "end": 0, "ms": 0, "errors": [], "found": 0}}


def bump():
    with LOCK:
        S["ver"] += 1


def log(msg, kind=""):
    with LOCK:
        S["feed"].insert(0, {"t": time.time(), "m": str(msg)[:300], "k": kind})
        del S["feed"][100:]


def warn(msg):
    with LOCK:
        if msg not in S["warn"]:
            S["warn"].append(msg)
    log(msg, "wn")


def clean(o):
    """Replace NaN/inf so the browser's JSON.parse never fails."""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {str(k): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, deque)):
        return [clean(v) for v in o]
    return o


def atomic_write(path, obj, private=False):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(clean(obj), f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    if private:
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass


def quarantine(path, why):
    dest = "%s.corrupt-%d" % (path, int(time.time()))
    try:
        os.replace(path, dest)
        warn("%s was unreadable (%s). Moved to %s and started fresh." % (os.path.basename(path), why, os.path.basename(dest)))
    except OSError as e:
        warn("Could not read %s (%s) and could not move it aside: %s" % (os.path.basename(path), why, e))


def valid_https(u, allow_local=True):
    try:
        p = urllib.parse.urlparse(u)
    except ValueError:
        return False
    if p.scheme == "https" and p.hostname:
        return True
    return allow_local and p.scheme == "http" and p.hostname in ("localhost", "127.0.0.1")


def clean_settings(inp, cur):
    out = dict(cur)
    for k, v in inp.items():
        if k in SCHEMA:
            sp = SCHEMA[k]
            if sp[0] is bool:
                if not isinstance(v, bool):
                    raise ValueError("%s must be on or off" % k)
                out[k] = v
            else:
                if isinstance(v, bool) or not isinstance(v, (int, float, str)):
                    raise ValueError("%s must be a number" % k)
                try:
                    x = float(v)
                except ValueError:
                    raise ValueError("%s must be a number" % k)
                if not math.isfinite(x) or x < sp[1] or x > sp[2]:
                    raise ValueError("%s must be between %s and %s" % (k, sp[1], sp[2]))
                out[k] = int(x) if sp[0] is int else x
        elif k == "ai_provider":
            if v not in ("anthropic", "openai"):
                raise ValueError("ai_provider must be anthropic or openai")
            out[k] = v
        elif k == "ai_model":
            if not isinstance(v, str) or not re.match(r"^[\w.\-:/]{1,100}$", v):
                raise ValueError("ai_model contains invalid characters")
            out[k] = v
        elif k == "ai_base":
            if not isinstance(v, str) or not valid_https(v.strip()) or len(v) > 200:
                raise ValueError("ai_base must be an https URL (http only for localhost)")
            out[k] = v.strip()
        elif k == "chains":
            if not isinstance(v, list) or len(v) > 20 or not all(isinstance(x, str) and re.match(r"^[a-z0-9\-]{2,30}$", x) for x in v):
                raise ValueError("chains must be a list of lowercase chain ids")
            out[k] = v
    return out


def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                raise ValueError("not a JSON object")
            S["settings"] = clean_settings(raw, dict(DEFAULTS))
            if set(raw) - set(DEFAULTS):       # old paper-trading keys are dropped on the next save
                log("Removed obsolete paper-trading settings from the config file", "wn")
        except Exception as e:
            quarantine(CONFIG_FILE, str(e)[:80])
    if os.path.exists(SECRETS_FILE):
        try:
            with open(SECRETS_FILE, encoding="utf-8") as f:
                raw = json.load(f)
            S["secrets"] = {k: v for k, v in raw.items() if k in ("ai_key", "sol_rpc", "cg_key") and isinstance(v, str)}
        except Exception as e:
            quarantine(SECRETS_FILE, str(e)[:80])


ENVN = {"ai_key": "DESK_AI_KEY", "sol_rpc": "DESK_SOL_RPC", "cg_key": "DESK_CG_KEY"}


def secret(name):
    return os.environ.get(ENVN[name]) or S["secrets"].get(name) or ""


def save_config():
    with LOCK:
        atomic_write(CONFIG_FILE, S["settings"])


def save_secrets():
    with LOCK:
        atomic_write(SECRETS_FILE, S["secrets"], private=True)


def scrub(m):
    m = str(m)
    for n in ENVN:
        v = secret(n)
        if v and len(v) >= 6:
            m = m.replace(v, "***")
    return re.sub(r"((?:api[_-]?)?key|token|secret)=[^&\s]+", r"\1=***", m, flags=re.I)


# ----------------------------------------------------------------------------- HTTP client with limits
class Prov:
    def __init__(self, name, rpm, note=""):
        self.name, self.rpm, self.note = name, rpm, note
        self.t = deque()
        self.lock = threading.Lock()
        self.status, self.last_ok, self.last_err = "idle", 0.0, ""
        self.blocked_until, self.calls, self.errors = 0.0, 0, 0

    def wait(self):
        while True:
            with self.lock:
                now = time.time()
                while self.t and now - self.t[0] >= 60:
                    self.t.popleft()
                w = max(0.0, self.blocked_until - now)
                if w == 0 and len(self.t) < self.rpm:
                    self.t.append(now)
                    return
                if w == 0:
                    w = 60 - (now - self.t[0])
            if w > 20:
                self.status = "rate_limited"
                raise ApiError("%s: rate limit reached, retry in about %ds" % (self.name, int(w)))
            if STOP.wait(min(w, 5.0)):
                raise ApiError("shutting down", True)

    def block(self, secs):
        with self.lock:
            self.blocked_until = max(self.blocked_until, time.time() + secs)
        self.status = "rate_limited"

    def good(self):
        self.status, self.last_ok = "ok", time.time()

    def bad(self, msg):
        self.status, self.last_err = "error", scrub(msg)
        self.errors += 1

    def snap(self):
        return {"name": self.name, "status": self.status, "last_ok": self.last_ok, "last_error": self.last_err,
                "calls": self.calls, "errors": self.errors, "rpm_limit": self.rpm, "note": self.note,
                "blocked_s": max(0, int(self.blocked_until - time.time()))}


P_DS = Prov("DexScreener (pairs/search)", 200, "documented 300/min; app stays below")
P_BOOST = Prov("DexScreener (discovery lists)", 45, "documented 60/min; app stays below")
P_GT = Prov("GeckoTerminal (trades, charts, new pools)", 10, "keyless public API; limit set in Settings")
P_RPC = Prov("Solana RPC (holders, wallet lookup)", 20, "public RPC is rate limited; optional custom RPC")
P_AI = Prov("AI provider", 12, "your own API key; billed by the provider")
PROVS = [P_DS, P_BOOST, P_GT, P_RPC, P_AI]


def http(prov, url, data=None, headers=None, timeout=15, retries=3, max_bytes=4000000):
    hdrs = {"User-Agent": "meme-desk/" + VERSION, "Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        hdrs["Content-Type"] = "application/json"
    last = None
    for attempt in range(retries):
        if STOP.is_set():
            raise ApiError("shutting down", True)
        prov.wait()
        prov.calls += 1
        try:
            req = urllib.request.Request(url, data=body, headers=hdrs, method="POST" if body is not None else "GET")
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read(max_bytes + 1)
            if len(raw) > max_bytes:
                raise ApiError("%s: response too large" % prov.name, True)
            try:
                out = json.loads(raw.decode("utf-8", "replace"))
            except ValueError:
                raise ApiError("%s: response was not valid JSON" % prov.name)
            prov.good()
            return out
        except ApiError as e:
            last = e
            if e.fatal:
                prov.bad(str(e))
                raise
        except urllib.error.HTTPError as e:
            code = e.code
            if code == 429:
                ra = e.headers.get("Retry-After") if e.headers else None
                try:
                    delay = float(ra)
                except (TypeError, ValueError):
                    delay = min(2 ** (attempt + 2), 60)
                prov.block(max(0.0, min(delay, 60.0)))
                last = ApiError("%s: rate limited (HTTP 429)" % prov.name)
            elif code >= 500:
                last = ApiError("%s: server error (HTTP %d)" % (prov.name, code))
            else:
                msg = ""
                try:
                    j = json.loads(e.read(2000).decode("utf-8", "replace"))
                    er = j.get("error")
                    msg = er.get("message", "") if isinstance(er, dict) else (er or j.get("message") or "")
                except Exception:
                    pass
                err = ApiError(scrub("%s: HTTP %d %s" % (prov.name, code, str(msg)[:200])).strip(), True)
                prov.bad(str(err))
                raise err
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, OSError) as e:
            last = ApiError(scrub("%s: connection problem (%s)" % (prov.name, str(getattr(e, "reason", e))[:120])))
        if attempt < retries - 1:
            STOP.wait(BACKOFF * (2 ** attempt))
    prov.bad(str(last))
    raise last


# ----------------------------------------------------------------------------- data providers
def fnum(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def s64(x):
    return None if x is None else str(x)[:64]


def safe_url(u, prefix=("https://",)):
    return u if isinstance(u, str) and len(u) < 300 and u.startswith(prefix) and not re.search(r"[\s<>\"']", u) else None


def norm(p):
    """DexScreener pair object -> coin dict. Missing fields stay None (never invented)."""
    if not isinstance(p, dict):
        return None
    base, quote = p.get("baseToken") or {}, p.get("quoteToken") or {}
    chain, addr, price = p.get("chainId"), base.get("address"), fnum(p.get("priceUsd"))
    if not isinstance(chain, str) or not isinstance(addr, str) or price is None or price <= 0:
        return None
    if not KEY_RE.match(chain + ":" + addr):
        return None
    pc, vol, tx, liq = p.get("priceChange") or {}, p.get("volume") or {}, p.get("txns") or {}, p.get("liquidity") or {}
    info = p.get("info") if isinstance(p.get("info"), dict) else {}
    links = []
    for w_ in info.get("websites") or []:
        u = safe_url(w_.get("url")) if isinstance(w_, dict) else None
        if u:
            links.append(["Website", u])
    for s_ in info.get("socials") or []:
        u = safe_url(s_.get("url")) if isinstance(s_, dict) else None
        if u:
            links.append([str(s_.get("type", "link"))[:20].capitalize(), u])

    def txn(w):
        t = tx.get(w) or {}
        return {"buys": fnum(t.get("buys")), "sells": fnum(t.get("sells"))}
    return {"key": chain + ":" + addr, "chain": chain, "addr": addr, "name": s64(base.get("name")),
            "sym": s64(base.get("symbol")) or "?", "price": price,
            "chg": {w: fnum(pc.get(w)) for w in W}, "vol": {w: fnum(vol.get(w)) for w in W},
            "tx": {w: txn(w) for w in W}, "liq": fnum(liq.get("usd")), "fdv": fnum(p.get("fdv")),
            "mcap": fnum(p.get("marketCap")), "created": fnum(p.get("pairCreatedAt")), "dex": s64(p.get("dexId")),
            "pair": s64(p.get("pairAddress")), "url": safe_url(p.get("url"), ("https://dexscreener.com/",)),
            "logo": safe_url(info.get("imageUrl"), ("https://cdn.dexscreener.com/",)), "links": links[:6],
            "boosts": fnum((p.get("boosts") or {}).get("active")) if isinstance(p.get("boosts"), dict) else None,
            "quote": {"sym": s64(quote.get("symbol"))}, "updated": time.time()}


def best_pairs(pairs, wanted=None):
    best = {}
    for p in pairs if isinstance(pairs, list) else []:
        c = norm(p)
        if not c or (wanted is not None and c["addr"].lower() not in wanted):
            continue
        old = best.get(c["key"])
        if old is None or (c["liq"] or 0) > (old["liq"] or 0):
            best[c["key"]] = c
    return best


def pump_info(c):
    """Pump.fun relation. Pump.fun has NO official public data API, so this is a labelled heuristic from public fields."""
    dex, ev = (c.get("dex") or "").lower(), []
    if c["chain"] != "solana":
        return None
    if "pump" in dex:
        ev.append("DexScreener lists the pool on '%s'" % c["dex"])
    suffix = c["addr"].endswith("pump")
    if suffix:
        ev.append("mint address ends with 'pump' (the naming pattern Pump.fun mints use; not proof)")
    if not ev:
        return None
    if dex == "pumpfun":
        status = "On the Pump.fun bonding curve (per DexScreener)"
    elif dex == "pumpswap":
        status = "Graduated to PumpSwap (per DexScreener)"
    elif suffix and "pump" not in dex:
        status = "Probably graduated: trades on %s (inferred from the mint pattern, unconfirmed)" % (c["dex"] or "another DEX")
    else:
        status = "Pump.fun related (status unclear)"
    return {"status": status, "evidence": ev, "bonding_pct": None,
            "note": "Bonding-curve progress and creator data are not available from the free sources used here."}


def ds_list(kind):
    path = {"top": "/token-boosts/top/v1", "latest": "/token-boosts/latest/v1", "profiles": "/token-profiles/latest/v1"}[kind]
    data = http(P_BOOST, DS_BASE + path)
    out = []
    for d in data if isinstance(data, list) else []:
        if isinstance(d, dict) and isinstance(d.get("chainId"), str) and isinstance(d.get("tokenAddress"), str):
            if KEY_RE.match(d["chainId"] + ":" + d["tokenAddress"]):
                out.append((d["chainId"], d["tokenAddress"]))
    return out


def ds_tokens(chain, addrs):
    return http(P_DS, "%s/tokens/v1/%s/%s" % (DS_BASE, chain, ",".join(addrs)))


def ds_search(q):
    data = http(P_DS, DS_BASE + "/latest/dex/search?q=" + urllib.parse.quote(q))
    return data.get("pairs") if isinstance(data, dict) else []


def gt_base():
    return GT_BASE_KEYED if secret("cg_key") else GT_BASE_FREE


def gt_headers():
    return {"x-cg-demo-api-key": secret("cg_key")} if secret("cg_key") else None


def gt_get(path, retries=2):
    P_GT.rpm = S["settings"]["gt_rpm"]
    return http(P_GT, gt_base() + path, headers=gt_headers(), retries=retries)


GT_NET = {"solana": "solana", "ethereum": "eth", "base": "base", "bsc": "bsc", "arbitrum": "arbitrum",
          "polygon": "polygon_pos", "avalanche": "avax", "optimism": "optimism", "sui": "sui-network",
          "ton": "ton", "tron": "tron"}


PATH_ID = re.compile(r"^[A-Za-z0-9_\-]{10,100}$")


def iso_to_ts(s):
    try:
        return float(calendar.timegm(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")))
    except (ValueError, TypeError):
        return None


def get_trades(c):
    """Real transactions for a pool from the documented GeckoTerminal/CoinGecko on-chain 'pool trades' endpoint (last 300 in 24h)."""
    net = GT_NET.get(c["chain"])
    if not net or not c.get("pair") or not PATH_ID.match(c["pair"]):
        raise ApiError("Transaction data is not available for this chain/pool", True)
    data = gt_get("/networks/%s/pools/%s/trades?token=%s" % (net, c["pair"], c["addr"]))
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ApiError("