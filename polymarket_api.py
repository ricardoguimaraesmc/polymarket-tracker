"""Read-only client for Polymarket public APIs."""
import json
import time
import urllib.parse
import urllib.request
import urllib.error
import sys

BASE = "https://data-api.polymarket.com"
_UA = {"User-Agent": "shark-money-tracker/3.0"}
MAX_PAGE = 10_000
MAX_OFFSET = 10_000


GAMMA_BASE = "https://gamma-api.polymarket.com"


def _gamma_get(path, params=None, timeout=20, retries=3):
    url = f"{GAMMA_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code not in (429, 500, 502, 503, 504) or attempt == retries - 1:
                raise
        except (urllib.error.URLError, TimeoutError) as e:
            last = e
            if attempt == retries - 1:
                raise
        time.sleep(0.75 * (2 ** attempt))
    raise last


def fetch_markets_by_condition_ids(condition_ids, chunk_size=100):
    """Batch-resolve Gamma market metadata for many condition IDs.

    Gamma accepts condition_ids as a repeated/comma-separated array parameter.
    Batching avoids one HTTP request per trade/market.
    """
    ids = []
    seen = set()
    for cid in condition_ids or []:
        cid = str(cid or "").strip()
        if cid and cid not in seen:
            seen.add(cid)
            ids.append(cid)

    result = {}
    for i in range(0, len(ids), max(1, int(chunk_size))):
        chunk = ids[i:i + max(1, int(chunk_size))]
        # condition_ids is documented as string[]; send repeated query
        # parameters (condition_ids=id1&condition_ids=id2&...) rather than
        # one comma-joined value, which Gamma may interpret as a single ID.
        params = [("condition_ids", cid) for cid in chunk]
        params.extend([
            ("limit", len(chunk)),
            ("include_tag", "true"),
        ])
        payload = _gamma_get("/markets", params)
        if isinstance(payload, list):
            for market in payload:
                cid = str(market.get("conditionId") or "").strip()
                if cid:
                    result[cid] = market
    return result


def fetch_sports_market_types():
    """Return the official sportsMarketType values published by Polymarket."""
    payload = _gamma_get("/sports/market-types", timeout=15, retries=2)
    if isinstance(payload, dict):
        values = payload.get("marketTypes") or []
    else:
        values = payload or []
    return [str(x).strip() for x in values if str(x).strip()]


def fetch_active_sports_markets(page_size=500, max_pages=4):
    """Load a bounded catalogue of active sports markets.

    This is the main performance optimization: instead of resolving Gamma
    metadata once per trade, we obtain the official sports catalogue in bulk.
    """
    types = fetch_sports_market_types()
    if not types:
        return []

    all_markets = []
    page_size = min(max(int(page_size), 1), 500)

    # Keep URLs reasonably sized by chunking sports types.
    for type_start in range(0, len(types), 25):
        type_chunk = types[type_start:type_start + 25]
        for page in range(max(1, int(max_pages))):
            params = [
                ("active", "true"),
                ("closed", "false"),
                ("limit", page_size),
                ("offset", page * page_size),
                ("include_tag", "true"),
            ]
            params.extend(("sports_market_types", x) for x in type_chunk)
            payload = _gamma_get("/markets", params, timeout=20, retries=2)
            rows = payload if isinstance(payload, list) else []
            all_markets.extend(rows)
            if len(rows) < page_size:
                break

    # De-duplicate by conditionId.
    out = {}
    for market in all_markets:
        cid = str(market.get("conditionId") or "").strip()
        if cid:
            out[cid] = market
    return list(out.values())


def _get(path, params=None, timeout=30, retries=4):
    url = f"{BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                # Respect Retry-After when supplied, but cap the wait so a
                # single bad request cannot stall a 5-minute polling cycle.
                try:
                    wait = float(e.headers.get("Retry-After", "2"))
                except Exception:
                    wait = 2.0
                wait = min(max(wait, 1.0), 8.0)
                if attempt == retries - 1:
                    raise
                time.sleep(wait)
                continue
            if e.code not in (500, 502, 503, 504) or attempt == retries - 1:
                raise
        except (urllib.error.URLError, TimeoutError) as e:
            last = e
            if attempt == retries - 1:
                raise
        time.sleep(1.0 * (2 ** attempt))
    raise last


def _trade_identity(t):
    # Keep the identity compatible with the existing tracker.db schema/state.
    # Changing this key would re-insert historical trades under new IDs.
    return (
        f"{t.get('transactionHash','')}:"
        f"{t.get('asset','')}:"
        f"{t.get('timestamp','')}"
    )


def _fetch_window(start, end, limit=MAX_PAGE, depth=0, max_depth=3):
    """Fetch a bounded time window without unbounded recursive pagination.

    The previous implementation fetched two 10k pages and recursively split
    whenever both were full. During very busy periods that recursion could
    explode into hundreds of HTTP calls and trigger Data API HTTP 429.

    This version never uses the 10k offset page. It fetches the time window
    directly and, only when a window is completely full, splits it a bounded
    number of times. A final full leaf is accepted rather than recursing
    forever. Requests are throttled slightly to stay comfortably below the
    public rate limit.
    """
    start = int(start)
    end = int(end)
    if end <= start:
        return []

    # Small client-side pacing. This is intentionally conservative because the
    # tracker is a 5-minute poller, not a high-frequency data harvester.
    time.sleep(0.15)

    first = _get("/trades", {
        "limit": min(int(limit), MAX_PAGE),
        "offset": 0,
        "takerOnly": "false",
        "start": start,
        "end": end,
    })
    if not isinstance(first, list):
        return []

    if len(first) < MAX_PAGE:
        return first

    if depth >= max_depth:
        print(
            f"[trades] warning: window {start}-{end} still has >= {MAX_PAGE:,} rows "
            f"after {max_depth} splits; keeping the newest {MAX_PAGE:,} rows",
            file=sys.stderr,
        )
        return first

    mid = (start + end) // 2
    if mid <= start or mid >= end:
        return first

    left = _fetch_window(start, mid, limit, depth + 1, max_depth)
    right = _fetch_window(mid, end, limit, depth + 1, max_depth)
    return left + right


def fetch_trades(limit=MAX_PAGE, offset=0, start=None, end=None):
    """Fetch public trades, including maker and taker fills.

    When start/end are supplied, crowded windows are split only to a bounded
    depth. This avoids the unbounded recursion that previously caused HTTP 429.
    """
    if start is not None or end is not None:
        now = int(time.time())
        start = now - 600 if start is None else int(start)
        end = now if end is None else int(end)
        rows = _fetch_window(start, end, min(int(limit), MAX_PAGE))
    else:
        rows = _get("/trades", {
            "limit": min(int(limit), MAX_PAGE),
            "offset": min(int(offset), MAX_OFFSET),
            "takerOnly": "false",
        })

    seen = set()
    out = []
    for t in rows or []:
        k = _trade_identity(t)
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def fetch_positions(wallet, limit=200):
    return _get("/positions", {
        "user": wallet,
        "limit": min(int(limit), 500),
        "sortBy": "CURRENT",
        "sortDirection": "DESC",
    })


def fetch_value(wallet):
    data = _get("/value", {"user": wallet})
    return data[0]["value"] if data else 0.0


def usd(trade):
    return float(trade.get("size", 0)) * float(trade.get("price", 0))


def trade_key(t):
    return _trade_identity(t)
