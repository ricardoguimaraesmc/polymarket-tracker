"""Read-only client for Polymarket public APIs."""
import json
import time
import urllib.parse
import urllib.request
import urllib.error

BASE = "https://data-api.polymarket.com"
_UA = {"User-Agent": "shark-money-tracker/3.0"}
MAX_PAGE = 10_000
MAX_OFFSET = 10_000


def _get(path, params=None, timeout=30, retries=3):
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
            if e.code not in (429, 500, 502, 503, 504) or attempt == retries - 1:
                raise
        except (urllib.error.URLError, TimeoutError, TimeoutError) as e:
            last = e
            if attempt == retries - 1:
                raise
        time.sleep(1.5 * (2 ** attempt))
    raise last


def _trade_identity(t):
    return (
        f"{t.get('transactionHash','')}:"
        f"{t.get('asset','')}:"
        f"{t.get('timestamp','')}:"
        f"{t.get('proxyWallet','')}"
    )


def _fetch_window(start, end, limit=MAX_PAGE):
    """Fetch a complete time window, recursively splitting when pagination is full.

    Polymarket's current Data API documents max limit=10,000 and max offset=10,000.
    Therefore one unsplit query can expose at most 20,000 rows. If both pages are
    full, split the time window and repeat. This removes the old shallow-lookback
    bottleneck for the global trade feed.
    """
    start = int(start)
    end = int(end)
    if end <= start:
        return []

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

    second = _get("/trades", {
        "limit": MAX_PAGE,
        "offset": MAX_OFFSET,
        "takerOnly": "false",
        "start": start,
        "end": end,
    })

    if not isinstance(second, list):
        second = []

    combined = first + second

    # If the second page is also full, there may be more than the endpoint's
    # 20,000-row offset budget in this window. Split by timestamp.
    if len(second) >= MAX_PAGE:
        mid = (start + end) // 2
        if mid <= start or mid >= end:
            return combined

        left = _fetch_window(start, mid, limit)
        right = _fetch_window(mid, end, limit)
        return left + right

    return combined


def fetch_trades(limit=MAX_PAGE, offset=0, start=None, end=None):
    """Fetch public trades, including maker and taker fills.

    When start/end are supplied, the window-aware paginator recursively splits
    crowded periods so the 10k limit / 10k offset ceiling does not truncate it.
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
