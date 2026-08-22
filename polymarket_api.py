"""Thin read-only client for Polymarket's public Data API (stdlib only)."""
import json
import time
import urllib.parse
import urllib.request

BASE = "https://data-api.polymarket.com"
_UA = {"User-Agent": "polymarket-whale-tracker/2.0"}

MAX_PAGE = 10000


def _get(path, params=None, timeout=30):
    url = f"{BASE}{path}"
    if params:
        clean = {k: v for k, v in params.items() if v is not None}
        url += "?" + urllib.parse.urlencode(clean)

    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_window(start, end, limit=MAX_PAGE):
    """Fetch all trades in a time window, recursively splitting crowded windows."""
    if start >= end:
        return []

    # First page: explicitly include both taker and maker trades.
    params = {
        "limit": min(int(limit), MAX_PAGE),
        "offset": 0,
        "takerOnly": "false",
        "start": int(start),
        "end": int(end),
    }

    first = _get("/trades", params)
    if not isinstance(first, list):
        return []

    rows = list(first)

    # If fewer than the page cap were returned, the whole window fits.
    if len(first) < MAX_PAGE:
        return rows

    # The endpoint caps offset at 10,000. Fetch the second page.
    second = _get(
        "/trades",
        {
            "limit": MAX_PAGE,
            "offset": MAX_PAGE,
            "takerOnly": "false",
            "start": int(start),
            "end": int(end),
        },
    )

    if not isinstance(second, list):
        second = []

    rows.extend(second)

    # If the second page is also full, the window may contain more than
    # the API's offset budget. Split the time window and fetch both halves.
    if len(second) >= MAX_PAGE:
        mid = (int(start) + int(end)) // 2
        if mid <= start or mid >= end:
            return rows

        left = _fetch_window(start, mid, limit)
        right = _fetch_window(mid + 1, end, limit)

        # Return the split-window result; dedupe below.
        rows = left + right

    # Deduplicate by the same trade identity used by the tracker.
    unique = {}
    for t in rows:
        key = trade_key(t)
        unique[key] = t

    return list(unique.values())


def fetch_trades(limit=MAX_PAGE, offset=0, start=None, end=None):
    """Fetch public trades.

    For normal calls, returns up to 10,000 recent trades.
    When start/end are supplied, automatically paginates/splits crowded
    time windows so a busy five-minute polling cycle does not silently
    lose trades at the 10,000/offset API caps.
    """
    limit = min(int(limit), MAX_PAGE)

    if start is not None or end is not None:
        now = int(time.time())
        start = now - 300 if start is None else int(start)
        end = now if end is None else int(end)

        return _fetch_window(start, end, limit)

    return _get(
        "/trades",
        {
            "limit": limit,
            "offset": min(max(int(offset), 0), MAX_PAGE),
            "takerOnly": "false",
        },
    )


def fetch_positions(wallet, limit=200):
    return _get("/positions", {"user": wallet, "limit": limit,
                               "sortBy": "CURRENT", "sortDirection": "DESC"})


def fetch_value(wallet):
    data = _get("/value", {"user": wallet})
    return data[0]["value"] if data else 0.0


def usd(trade):
    return float(trade.get("size", 0)) * float(trade.get("price", 0))


def trade_key(t):
    return f"{t.get('transactionHash','')}:{t.get('asset','')}:{t.get('timestamp','')}"
