#!/usr/bin/env python3
"""
🐋 Polymarket Whale Tracker v2  (read-only & legit)
==================================================
Pantau trade gede di Polymarket, follow smart-money, deteksi spike, simpan
history, dan kirim alert ke Telegram/Discord. Cuma baca data publik Polymarket
Data API — gak nyentuh transaksi siapapun.

Commands:
  scan         Sekali scan trade terbaru, flag whale
  watch        Monitor real-time di terminal (loop)
  poll         Sekali jalan: simpan ke DB + kirim alert (buat cron / GitHub Actions)
  wallet       Analisa 1 wallet (saldo, posisi, PnL, trade terakhir)
  leaderboard  Top trader by volume dari N trade terakhir
  score        Skor smart-money sebuah wallet (PnL & win-rate)
  watchlist    Kelola daftar wallet favorit (add/remove/list/pnl)
  consensus    Cari market di mana beberapa whale beli sisi yang sama
  digest       Ringkasan periode (top whale, market terpanas) + kirim ke alert

Contoh:
  python3 whale_tracker.py scan --min-usd 1000
  python3 whale_tracker.py watch --min-usd 5000 --smart --min-pnl 5000 --min-winrate 0.55
  python3 whale_tracker.py poll --min-usd 5000 --smart --spike-usd 20000
  python3 whale_tracker.py wallet 0xABC...
  python3 whale_tracker.py watchlist add 0xABC... --label "OG trader"
  python3 whale_tracker.py watchlist pnl

Alert config (env var):
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DISCORD_WEBHOOK_URL
"""
import argparse
import sys
import time
from datetime import datetime, timezone

import db as DB
import notifier
from polymarket_api import (fetch_trades, fetch_positions, fetch_value, usd, trade_key,
                              fetch_markets_by_condition_ids)
from smartmoney import score_wallet, is_smart


# ----------------------------- Shark Money filters -----------------------------
MIN_ENTRY_ODD = 1.50
MIN_ENTRY_PRICE = 1.0 / MIN_ENTRY_ODD
GAMMA_BASE = "https://gamma-api.polymarket.com"
MARKET_CACHE_FILE = ".polymarket_market_cache.json"

# Common sports/e-sports/virtual-sports hints. Gamma metadata remains the
# authoritative classifier; these terms are only useful for diagnostics and
# future fallbacks and MUST NOT turn an unknown market into an alert.
SPORTS_FALLBACK_TERMS = (
    "football", "soccer", "basketball", "tennis", "baseball", "hockey",
    "volleyball", "rugby", "cricket", "golf", "boxing", "mma", "ufc",
    "formula 1", "formula1", "f1", "nascar", "motogp", "cycling",
    "handball", "darts", "snooker", "table tennis", "ping pong",
    "badminton", "wrestling", "esports", "e-sports", "counter-strike",
    "counter strike", "valorant", "league of legends", "lol esports",
    "dota", "dota 2", "rocket league", "overwatch", "call of duty",
    "starcraft", "rainbow six", "mlbb", "virtual football",
    "virtual soccer", "virtual basketball", "virtual tennis", "virtual sports"
)

TRANSLATIONS = {
    "moneyline": "vencedor da partida", "match winner": "vencedor da partida",
    "game winner": "vencedor do jogo", "winner": "vencedor",
    "to win": "para vencer", "win": "vencer", "draw": "empate",
    "tie": "empate", "over": "mais de", "under": "menos de",
    "both teams to score": "ambas marcam", "yes": "sim", "no": "não",
    "spread": "handicap", "asian handicap": "handicap asiático",
    "handicap": "handicap", "total goals": "total de gols",
    "total points": "total de pontos", "total games": "total de games",
    "total sets": "total de sets", "total maps": "total de mapas",
    "match": "partida", "game": "jogo", "first half": "primeiro tempo",
    "second half": "segundo tempo", "quarter": "quarto",
    "semi-final": "semifinal", "semifinals": "semifinais",
    "quarter-final": "quartas de final", "quarterfinal": "quartas de final",
    "playoffs": "playoffs", "qualifier": "qualificatória",
    "qualifiers": "qualificatórias", "correct score": "placar exato",
    "win or draw": "vitória ou empate", "double chance": "dupla chance",
    "first team to score": "primeiro time a marcar",
    "last team to score": "último time a marcar",
    "to qualify": "para se classificar", "advance": "avançar",
    "series winner": "vencedor da série", "race winner": "vencedor da corrida",
    "podium": "pódio", "set winner": "vencedor do set",
    "map winner": "vencedor do mapa", "round winner": "vencedor do round",
}

def _load_market_cache():
    try:
        import json
        with open(MARKET_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

_MARKET_CACHE = _load_market_cache()
_RUN_MARKET_CACHE = {}
_MARKET_FAIL_CACHE = {}
def _text_sports_hint(t):
    text = " ".join(
        str(t.get(k) or "")
        for k in ("title", "eventSlug", "slug", "outcome")
    ).lower()
    return any(term in text for term in SPORTS_FALLBACK_TERMS)


def _prime_market_cache(condition_ids):
    """Resolve many markets in a few Gamma calls and cache the results.

    The Gamma API supports condition_ids as a string-array query parameter.
    We batch in groups of 50 to keep URLs comfortably sized.
    """
    ids = []
    seen = set()
    for x in condition_ids or []:
        cid = str(x or "").strip()
        if cid and cid not in seen:
            seen.add(cid)
            ids.append(cid)

    missing = [cid for cid in ids
               if cid not in _MARKET_CACHE and cid not in _RUN_MARKET_CACHE
               and cid not in _MARKET_FAIL_CACHE]
    if not missing:
        return 0

    loaded = 0
    try:
        for i in range(0, len(missing), 100):
            batch_ids = missing[i:i + 100]
            batch = fetch_markets_by_condition_ids(batch_ids, chunk_size=100)
            for cid in batch_ids:
                meta = batch.get(cid)
                if meta is not None:
                    _RUN_MARKET_CACHE[cid] = meta
                    _MARKET_CACHE[cid] = meta
                    loaded += 1
                else:
                    _MARKET_FAIL_CACHE[cid] = time.time()
        if loaded:
            _save_market_cache()
    except Exception as exc:
        # Fail closed: an API failure must not turn an unknown market into a
        # sports alert. Mark this batch as failed for a short period so the
        # same run does not hammer Gamma repeatedly.
        now = time.time()
        for cid in missing:
            _MARKET_FAIL_CACHE[cid] = now
        print(f"[gamma] batch metadata warning: {exc}", file=sys.stderr)
    return loaded


def _market_for_trade(t):
    cid = str(t.get("conditionId") or "").strip()
    if not cid:
        return None
    if cid in _RUN_MARKET_CACHE:
        return _RUN_MARKET_CACHE[cid]
    cached = _MARKET_CACHE.get(cid)
    if cached:
        _RUN_MARKET_CACHE[cid] = cached
        return cached
    if cid in _MARKET_FAIL_CACHE and time.time() - _MARKET_FAIL_CACHE[cid] < 120:
        return None
    _prime_market_cache([cid])
    return _RUN_MARKET_CACHE.get(cid)


def _save_market_cache():
    try:
        import json, os
        if len(_MARKET_CACHE) > 5000:
            keep = list(_MARKET_CACHE.items())[-5000:]
            _MARKET_CACHE.clear()
            _MARKET_CACHE.update(keep)
        tmp = MARKET_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_MARKET_CACHE, f, ensure_ascii=False)
        os.replace(tmp, MARKET_CACHE_FILE)
    except Exception:
        pass


def _is_sports_metadata(meta):
    if not meta:
        return False
    category = str(meta.get("category") or "").strip().lower()
    subcategory = str(meta.get("subcategory") or "").strip().lower()
    market_type = str(meta.get("marketType") or "").strip().lower()
    sports_market_type = str(meta.get("sportsMarketType") or "").strip().lower()

    # Official sports fields are authoritative. Gamma exposes these on sports
    # markets, including e-sports and virtual sports.
    if sports_market_type or meta.get("gameId") or meta.get("teamAID") or meta.get("teamBID"):
        return True

    text = " ".join([category, subcategory, market_type])
    if "sport" in text or "esport" in text:
        return True

    for tag in meta.get("tags") or []:
        if isinstance(tag, dict):
            value = " ".join(str(tag.get(k) or "") for k in ("slug", "label", "name")).lower()
        else:
            value = str(tag).lower()
        if "sport" in value or "esport" in value:
            return True
    return False


def is_sports_trade(t):
    """Fail-closed classifier using cached official Gamma market metadata."""
    cid = str(t.get("conditionId") or "").strip()
    if not cid:
        return False
    meta = _MARKET_CACHE.get(cid) or _RUN_MARKET_CACHE.get(cid)
    if meta is None:
        meta = _market_for_trade(t)
    return _is_sports_metadata(meta)

def implied_odd(t):
    try:
        price = float(t.get("price", 0))
        if price <= 0:
            return None
        return 1.0 / price
    except Exception:
        return None

def odd_allowed(t):
    odd = implied_odd(t)
    return odd is not None and odd >= MIN_ENTRY_ODD

def _parse_iso_ts(value):
    if not value:
        return None
    try:
        s = str(value).replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None

def event_phase(t):
    """Classify PRÉ-LIVE/AO VIVO from cached official Gamma sports timing."""
    meta = _market_for_trade(t) or {}
    now_ts = int(time.time())

    # Gamma sports market data exposes event/game start time. Its parent event
    # can also expose a live flag. Avoid one CLOB HTTP request per alert.
    for ev in meta.get("events") or []:
        if ev.get("live") is True:
            return "🔴 AO VIVO"
    if meta.get("live") is True:
        return "🔴 AO VIVO"

    for key in ("eventStartTime", "gameStartTime", "event_start_time", "game_start_time",
                "startDate", "start_date"):
        start = _parse_iso_ts(meta.get(key))
        if start is not None:
            return "🔴 AO VIVO" if now_ts >= start else "🟢 PRÉ-LIVE"

    return "⚪ STATUS INDETERMINADO"

def _translate_text(text):
    """Lightweight sports-betting translation; names/team names are preserved."""
    if not text:
        return ""
    import re
    out = str(text)
    # Long phrases first so shorter replacements don't break them.
    for src in sorted(TRANSLATIONS, key=len, reverse=True):
        out = re.sub(r"(?i)(?<![\w])" + re.escape(src) + r"(?![\w])",
                     TRANSLATIONS[src], out)
    return out

def translated_outcome(t):
    return _translate_text(t.get("outcome") or "?")

def translated_title(t):
    return _translate_text(t.get("title") or "Mercado esportivo")

def trade_context(t):
    meta = _market_for_trade(t) or {}
    phase = event_phase(t)
    return {
        "meta": meta,
        "phase": phase,
        "category": str(meta.get("category") or "Sports"),
        "title_pt": translated_title(t),
        "outcome_pt": translated_outcome(t),
        "odd": implied_odd(t),
    }

def _eligible_entry(t):
    """Alert/accumulation eligibility: sports + BUY + odd >= 1.50."""
    if (t.get("side") or "").upper() != "BUY":
        return False
    if not odd_allowed(t):
        return False
    return is_sports_trade(t)



# ----------------------------- formatting -----------------------------
def fmt_money(x):
    return f"${x:,.2f}"


def short(a):
    return f"{a[:6]}...{a[-4:]}" if a and len(a) > 12 else a


def ts_str(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def name_of(t):
    return t.get("name") or t.get("pseudonym") or short(t.get("proxyWallet", ""))


def trade_line(t):
    arrow = "🟢 BUY " if t.get("side") == "BUY" else "🔴 SELL"
    return (f"{arrow} {fmt_money(usd(t)):>12}  {name_of(t)[:20]:<20}  "
            f"{(t.get('outcome') or '?')[:8]:<8} @ {float(t.get('price',0)):.3f} | {(t.get('title') or '')[:50]}")


def translate_market(t):
    """Return the market/question in Portuguese without translating team/player names."""
    meta = _market_for_trade(t) or {}
    raw = (
        meta.get("question")
        or meta.get("groupItemTitle")
        or meta.get("marketType")
        or "Mercado esportivo"
    )
    return _translate_text(raw)

def _md_dynamic(text):
    """Escape dynamic content for Telegram legacy Markdown."""
    s = "" if text is None else str(text)
    for ch in ("\\", "_", "*", "[", "]", "`"):
        s = s.replace(ch, "\\" + ch)
    return s

def alert_text(t, reasons, score=None):
    arrow = "🟢 *BUY*" if (t.get("side") or "").upper() == "BUY" else "🔴 *SELL*"
    price = float(t.get("price", 0) or 0)
    odd = (1.0 / price) if price > 0 else None
    phase = event_phase(t)
    market = _md_dynamic(translate_market(t))
    outcome = _md_dynamic(_translate_text(t.get("outcome", "?")))
    title = _md_dynamic(_translate_text(t.get("title", "")))
    tags = " ".join({
        "whale": "🐋WHALE", "smart": "🧠SMART-MONEY",
        "watchlist": "⭐WATCHLIST", "spike": "📈SPIKE",
        "consensus": "🎯CONSENSUS"
    }.get(r, r) for r in reasons)

    lines = [
        tags or "🐋 *SHARK MONEY*",
        f"{arrow} *{fmt_money(usd(t))}*",
        f"🏟️ *Evento:* {title}",
        f"🎯 *Mercado:* {market}",
        f"📌 *Entrada:* {outcome}",
        f"📊 *Odd:* {odd:.2f}" if odd is not None else "📊 *Odd:* —",
        f"💵 *Preço Polymarket:* {price:.3f} ({price*100:.1f}%)",
        f"⏱️ *Status:* {phase}",
        f"👤 *Trader:* `{_md_dynamic(name_of(t))}`",
    ]
    if score:
        lines.append(
            f"📈 *Trader:* PnL {fmt_money(score['realized_pnl'])} | "
            f"winrate {score['winrate']*100:.0f}% ({score['n_closed']} fechadas)"
        )
    wallet = t.get("proxyWallet", "")
    if wallet:
        lines.append(f"https://polymarket.com/profile/{wallet}")
    return "\n".join(lines)


# ----------------------------- commands -----------------------------
def cmd_scan(args):
    trades = fetch_trades(limit=args.lookback, start=int(time.time()) - 600, end=int(time.time()))
    whales = [t for t in trades if usd(t) >= args.min_usd]
    whales.sort(key=usd, reverse=True)
    print(f"\n🐋 WHALE SCAN — {len(whales)} trade >= {fmt_money(args.min_usd)} "
          f"(dari {len(trades)} terakhir)\n" + "-" * 104)
    for t in whales[: args.top]:
        print(trade_line(t))
    print("-" * 104)
    print(f"Total volume whale: {fmt_money(sum(usd(t) for t in whales))}\n")


def cmd_watch(args):
    conn = DB.connect(args.db)
    follow = {a.strip().lower() for a in args.follow.split(",") if a.strip()} if args.follow else None
    seen = set()
    score_cache = {}
    chans = notifier.configured()
    print(f"👀 WATCH — min {fmt_money(args.min_usd)}"
          + (f", smart-money(PnL>={fmt_money(args.min_pnl)},WR>={args.min_winrate})" if args.smart else "")
          + (f", follow {len(follow)}" if follow else "")
          + (f", alerts→{chans}" if chans else ", alerts→terminal only")
          + f", every {args.interval}s. Ctrl+C to stop.\n")
    try:
        while True:
            try:
                now = int(time.time())
                trades = fetch_trades(
                    limit=args.lookback,
                    start=now - max(120, args.interval * 4),
                    end=now,
                )
            except Exception as e:
                print(f"⚠️ fetch error: {e}", file=sys.stderr)
                time.sleep(args.interval)
                continue
            for t in sorted(trades, key=lambda x: x.get("timestamp", 0)):
                k = trade_key(t)
                if k in seen:
                    continue
                seen.add(k)

                # Keep the interactive watcher consistent with poll:
                # BUY alerts require sports + odd >= 1.50; SELL requires sports.
                side = (t.get("side") or "").upper()
                if side == "BUY":
                    if not _eligible_entry(t):
                        continue
                elif side == "SELL":
                    if not is_sports_trade(t):
                        continue
                else:
                    continue

                DB.insert_trade(conn, t, usd(t))
                reasons, score = _evaluate(conn, t, args, follow, score_cache)
                if reasons:
                    print(f"[{ts_str(t.get('timestamp'))}] {trade_line(t)}  <= {','.join(reasons)}")
                    if chans:
                        notifier.notify(alert_text(t, reasons, score))
            conn.commit()
            if len(seen) > 20000:
                seen = set(list(seen)[-10000:])
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n👋 Stopped.")
    finally:
        conn.commit()
        conn.close()

def accumulated_bet(conn, t):
    """Soma BUYs da mesma carteira na mesma seleção/mercado."""
    wallet = (t.get("proxyWallet") or "").lower()
    condition_id = t.get("conditionId")
    outcome = t.get("outcome")

    if not wallet or not condition_id or not outcome:
        return 0.0, 0

    row = conn.execute(
        """
        SELECT COALESCE(SUM(usd), 0) AS total, COUNT(*) AS n
        FROM trades
        WHERE wallet = ?
          AND condition_id = ?
          AND outcome = ?
          AND side = 'BUY'
        """,
        (wallet, condition_id, outcome),
    ).fetchone()

    return float(row["total"] or 0), int(row["n"] or 0)
def cmd_poll(args):
    """Monitora BUY acumulado somente em esportes com odd >= 1,50."""
    conn = DB.connect(args.db)

    # Aproximadamente R$ 990 mil
    ACCUMULATED_MIN_USD = 190_000

    # GitHub Actions roda a cada ~5 min. Mantemos uma sobreposição de 7 min
    # para tolerar pequenos atrasos entre execuções.
    now = int(time.time())
    fetch_started = time.time()
    trades = fetch_trades(
        limit=args.lookback,
        start=now - 420,
        end=now,
    )
    fetch_elapsed = time.time() - fetch_started
    print(f"[poll] trades={len(trades)} fetched in {fetch_elapsed:.1f}s")

    new_trades = []
    buy_candidates = []
    sell_candidates = []
    metadata_needed = set()

    for t in trades:
        side = (t.get("side") or "").upper()
        cid = str(t.get("conditionId") or "").strip()
        if not cid:
            continue

        if side == "BUY":
            if not odd_allowed(t):
                continue
            buy_candidates.append(t)
            # Resolve every uncached candidate by conditionId. This is deliberate:
            # title/slug keyword heuristics can miss esports, virtual sports,
            # team-only names, and non-English markets. Batch requests keep this
            # bounded while preserving a fail-closed official metadata classifier.
            if cid not in _MARKET_CACHE and cid not in _RUN_MARKET_CACHE:
                metadata_needed.add(cid)

        elif side == "SELL":
            sell_candidates.append(t)

    # Resolve BUY candidates in batches. This is the only metadata work needed
    # for normal entries and avoids one HTTP request per trade.
    if metadata_needed:
        meta_started = time.time()
        print(f"[poll] resolving {len(metadata_needed)} uncached BUY markets")
        loaded = _prime_market_cache(metadata_needed)
        print(f"[poll] metadata resolved: {loaded}/{len(metadata_needed)} in {time.time() - meta_started:.1f}s")

    # For SELLs, only markets whose existing eligible BUY total is already at
    # least $190k need metadata. Resolve those IDs in one batch too.
    sell_metadata_needed = set()
    sell_qualified = []
    for t in sell_candidates:
        wallet = (t.get("proxyWallet") or "").lower()
        condition_id = t.get("conditionId") or ""
        outcome = t.get("outcome") or ""
        if not wallet or not condition_id or not outcome:
            continue
        row = conn.execute(
            """SELECT COALESCE(SUM(usd),0) AS total FROM trades
               WHERE wallet=? AND condition_id=? AND outcome=?
               AND side='BUY' AND price <= ?""",
            (wallet, condition_id, outcome, MIN_ENTRY_PRICE),
        ).fetchone()
        if float(row["total"] or 0) >= ACCUMULATED_MIN_USD:
            sell_qualified.append(t)
            if condition_id not in _MARKET_CACHE and condition_id not in _RUN_MARKET_CACHE:
                sell_metadata_needed.add(condition_id)
    if sell_metadata_needed:
        sell_started = time.time()
        print(f"[poll] resolving {len(sell_metadata_needed)} uncached SELL markets")
        loaded = _prime_market_cache(sell_metadata_needed)
        print(f"[poll] SELL metadata resolved: {loaded}/{len(sell_metadata_needed)} in {time.time() - sell_started:.1f}s")

    # Insert eligible BUYs first.
    for t in buy_candidates:
        if not _eligible_entry(t):
            continue

        tx_key = trade_key(t)

        exists = conn.execute(
            "SELECT 1 FROM trades WHERE tx_key=?",
            (tx_key,),
        ).fetchone()

        if not exists:
            DB.insert_trade(conn, t, usd(t))
            new_trades.append(t)

    # Only consider SELLs belonging to a position that has already crossed
    # US$190k in eligible BUYs.
    for t in sell_qualified:
        wallet = (t.get("proxyWallet") or "").lower()
        condition_id = t.get("conditionId") or ""
        outcome = t.get("outcome") or ""
        if not wallet or not condition_id or not outcome:
            continue

        if not is_sports_trade(t):
            continue

        tx_key = trade_key(t)
        exists = conn.execute(
            "SELECT 1 FROM trades WHERE tx_key=?",
            (tx_key,),
        ).fetchone()
        if not exists:
            DB.insert_trade(conn, t, usd(t))
            new_trades.append(t)

    conn.commit()
    new_alerts = 0

    # =========================================================
    # 1) HISTÓRICO E AGRUPAMENTO
    # =========================================================
    groups = conn.execute(
        """
        SELECT
            wallet,
            condition_id,
            outcome,
            COALESCE(SUM(CASE WHEN side='BUY' AND price <= ? THEN usd ELSE 0 END), 0) AS buy_total,
            COALESCE(SUM(CASE WHEN side='SELL' THEN usd ELSE 0 END), 0) AS sell_total,
            COUNT(CASE WHEN side='BUY' AND price <= ? THEN 1 END) AS buy_count
        FROM trades
        WHERE wallet IS NOT NULL AND wallet != ''
          AND condition_id IS NOT NULL AND condition_id != ''
          AND outcome IS NOT NULL AND outcome != ''
        GROUP BY wallet, condition_id, outcome
        HAVING buy_total >= ?
        """,
        (MIN_ENTRY_PRICE, MIN_ENTRY_PRICE, ACCUMULATED_MIN_USD),
    ).fetchall()

    # Resolve only markets that actually crossed the accumulation threshold
    # and are not already present in the sports catalogue/cache.
    group_missing = []
    for g in groups:
        cid = str(g["condition_id"] or "").strip()
        if cid and cid not in _MARKET_CACHE and cid not in _RUN_MARKET_CACHE:
            group_missing.append(cid)
    if group_missing:
        _prime_market_cache(group_missing)

    # =========================================================
    # 2) ALERTA DE BUY ACUMULADO
    # =========================================================
    for g in groups:
        wallet = g["wallet"]
        condition_id = g["condition_id"]
        outcome = g["outcome"]
        buy_total = float(g["buy_total"] or 0)
        sell_total = float(g["sell_total"] or 0)
        buy_count = int(g["buy_count"] or 0)

        latest = conn.execute(
            """
            SELECT *
            FROM trades
            WHERE wallet=? AND condition_id=? AND outcome=?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (wallet, condition_id, outcome),
        ).fetchone()

        if not latest:
            continue

        # Historical rows do not carry eventSlug in the current DB schema,
        # so resolve the market directly by conditionId.
        latest_t = {
            "conditionId": condition_id,
            "title": latest["title"],
            "outcome": latest["outcome"],
            "price": latest["price"],
            "side": latest["side"],
            "timestamp": latest["ts"],
        }
        if not is_sports_trade(latest_t):
            continue

        alert_key = f"accumulated:{wallet}:{condition_id}:{outcome}"
        if DB.already_alerted(conn, alert_key):
            continue

        # Descobre exatamente o BUY que cruzou US$ 190 mil.
        buy_rows = conn.execute(
            """
            SELECT ts, usd, price, title, outcome
            FROM trades
            WHERE wallet=? AND condition_id=? AND outcome=? AND side='BUY' AND price <= ?
            ORDER BY ts ASC
            """,
            (wallet, condition_id, outcome, MIN_ENTRY_PRICE),
        ).fetchall()

        running_buy = 0.0
        threshold_ts = None
        threshold_price = None

        for br in buy_rows:
            running_buy += float(br["usd"] or 0)
            if running_buy >= ACCUMULATED_MIN_USD:
                threshold_ts = int(br["ts"])
                threshold_price = float(br["price"] or 0)
                break

        if threshold_ts is None:
            continue

        threshold_trade = {
            "conditionId": condition_id,
            "title": latest["title"],
            "outcome": outcome,
            "price": threshold_price,
            "side": "BUY",
            "timestamp": threshold_ts,
        }

        if not odd_allowed(threshold_trade):
            continue

        ctx = trade_context(threshold_trade)
        odd = ctx["odd"]
        phase = ctx["phase"]

        txt = (
            f"🐋 *SHARK MONEY — ENTRADA ACUMULADA*\n\n"
            f"🏆 *Evento:* {_md_dynamic(ctx['title_pt'])}\n"
            f"🎯 *Entrada:* {_md_dynamic(ctx['outcome_pt'])}\n"
            f"📊 *Odd:* {odd:.2f}\n"
            f"💵 *Preço/Probabilidade:* {threshold_price:.3f} ({threshold_price*100:.1f}%)\n"
            f"⏱️ *Momento:* {phase}\n"
            f"💰 *BUY acumulado:* ${buy_total:,.0f}\n"
            f"🧾 *Entradas BUY:* {buy_count}\n"
            f"🔴 *SELL acumulado:* ${sell_total:,.0f}\n"
            f"📊 *Posição estimada:* ${buy_total - sell_total:,.0f}\n"
            f"👛 *Carteira:* `{wallet[:12]}...`\n\n"
            f"🚨 *A carteira cruzou ${ACCUMULATED_MIN_USD:,.0f} "
            f"em entradas com odd mínima de {MIN_ENTRY_ODD:.2f}.*"
        )

        chans = notifier.notify(txt)
        if not chans:
            print("[alert] Telegram/Discord delivery failed; alert remains pending", file=sys.stderr)
            continue
        DB.mark_alerted(conn, alert_key, "accumulated")
        new_alerts += 1

        print(
            f"ALERT [accumulated] {ctx['title_pt'][:50]} | "
            f"{ctx['outcome_pt']} | odd {odd:.2f} | {phase} | "
            f"BUY ${buy_total:,.0f} -> {chans or 'terminal'}"
        )

    # =========================================================
    # 3) NOVOS SELL / REDUÇÃO DE POSIÇÃO
    # =========================================================
    for t in sorted(new_trades, key=lambda x: x.get("timestamp", 0)):
        if (t.get("side") or "").upper() != "SELL":
            continue

        wallet = (t.get("proxyWallet") or "").lower()
        condition_id = t.get("conditionId") or ""
        outcome = t.get("outcome") or ""
        sell_ts = int(t.get("timestamp", 0))

        if not wallet or not condition_id or not outcome:
            continue

        # A posição só é considerada "Shark" se os BUYs elegíveis
        # (esporte + odd >= 1,50) cruzaram o limite.
        buy_rows = conn.execute(
            """
            SELECT ts, usd, price
            FROM trades
            WHERE wallet=? AND condition_id=? AND outcome=? AND side='BUY' AND price <= ?
            ORDER BY ts ASC
            """,
            (wallet, condition_id, outcome, MIN_ENTRY_PRICE),
        ).fetchall()

        running_buy = 0.0
        threshold_ts = None
        for br in buy_rows:
            running_buy += float(br["usd"] or 0)
            if running_buy >= ACCUMULATED_MIN_USD:
                threshold_ts = int(br["ts"])
                break

        if threshold_ts is None or sell_ts <= threshold_ts:
            continue

        sell_key = f"reverse:{trade_key(t)}"
        if DB.already_alerted(conn, sell_key):
            continue

        totals = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN side='BUY' AND ts<=? THEN usd ELSE 0 END),0) AS bought,
                COALESCE(SUM(CASE WHEN side='SELL' AND ts<=? THEN usd ELSE 0 END),0) AS sold
            FROM trades
            WHERE wallet=? AND condition_id=? AND outcome=?
            """,
            (sell_ts, sell_ts, wallet, condition_id, outcome),
        ).fetchone()

        bought = float(totals["bought"] or 0)
        sold = float(totals["sold"] or 0)
        position = bought - sold

        ctx = trade_context(t)
        odd = ctx["odd"]

        txt = (
            f"🔴 *SHARK MONEY — REDUÇÃO DE POSIÇÃO*\n\n"
            f"🏆 *Evento:* {_md_dynamic(ctx['title_pt'])}\n"
            f"🎯 *Entrada:* {_md_dynamic(ctx['outcome_pt'])}\n"
            f"📊 *Odd atual:* {odd:.2f}" if odd else
            f"📊 *Odd atual:* indisponível"
        )
        txt += (
            f"\n💵 *Preço/Probabilidade:* {float(t.get('price',0)):.3f} "
            f"({float(t.get('price',0))*100:.1f}%)"
            f"\n⏱️ *Momento:* {ctx['phase']}"
            f"\n👛 *Carteira:* `{wallet[:12]}...`"
            f"\n\n💰 *BUY acumulado:* ${bought:,.0f}"
            f"\n🔴 *SELL acumulado:* ${sold:,.0f}"
            f"\n📊 *Posição estimada:* ${position:,.0f}"
            f"\n🔻 *Novo SELL:* ${usd(t):,.0f}"
            f"\n\n⚠️ *Essa carteira havia cruzado "
            f"${ACCUMULATED_MIN_USD:,.0f} em entradas elegíveis.*"
        )

        chans = notifier.notify(txt)
        if not chans:
            print("[alert] Telegram/Discord delivery failed; SELL alert remains pending", file=sys.stderr)
            continue
        DB.mark_alerted(conn, sell_key, "reverse")
        new_alerts += 1

        print(
            f"ALERT [reverse] {ctx['title_pt'][:50]} | "
            f"{ctx['outcome_pt']} | {ctx['phase']} | "
            f"SELL ${usd(t):,.0f} -> {chans or 'terminal'}"
        )

    conn.commit()
    conn.close()

    print(
        f"\n✅ poll done. {len(trades)} trades scanned, "
        f"{new_alerts} new alerts sent."
    )

def cmd_consensus(args):
    """Find markets where several whales bought the same outcome recently."""
    conn = DB.connect(args.db)
    now = int(time.time())
    trades = fetch_trades(
        limit=args.lookback,
        start=now - args.window * 60,
        end=now,
    )
    for t in trades:
        DB.insert_trade(conn, t, usd(t))
    conn.commit()
    since = int(time.time()) - args.window * 60
    groups = DB.consensus_groups(conn, since, args.min_usd, args.wallets)
    print(f"\n🎯 CONSENSUS — market dgn >= {args.wallets} wallet beli sisi sama "
          f"(trade >= {fmt_money(args.min_usd)}, window {args.window} menit)\n" + "-" * 96)
    if not groups:
        print("(belum ada — coba perpanjang --window atau turunin --min-usd)")
    for g in groups[: args.top]:
        print(f"\n  {g['n_wallets']} wallets → *{g['outcome']}*  |  total {fmt_money(g['vol'])}")
        print(f"  {g['title']}")
        for b in DB.consensus_wallets(conn, g["condition_id"], g["outcome"], since, args.min_usd):
            print(f"    • {(b['name'] or short(b['wallet']))[:24]:<24} {fmt_money(b['vol']):>12}")
    print()
    conn.close()


def cmd_digest(args):
    """Periodic summary of stored history. --send pushes it to Telegram/Discord."""
    conn = DB.connect(args.db)
    since = int(time.time()) - args.hours * 3600
    s = DB.digest_stats(conn, since)
    lines = [f"📊 *WHALE DIGEST — last {args.hours}h*",
             f"Volume tracked: {fmt_money(s['volume'])}  |  {s['n_trades']} trades  |  "
             f"{s['n_wallets']} wallets  |  {s['n_alerts']} alerts sent", ""]
    if s["top_trades"]:
        lines.append("🐋 *Top trades:*")
        for t in s["top_trades"]:
            side = "🟢" if t["side"] == "BUY" else "🔴"
            lines.append(f"  {side} {fmt_money(t['usd'])} — {t['outcome']} @ {t['price']:.2f} | "
                         f"{(t['title'] or '')[:48]}")
    if s["top_markets"]:
        lines.append("\n🔥 *Hottest markets:*")
        for m in s["top_markets"]:
            lines.append(f"  {fmt_money(m['vol'])} ({m['n']} trades) | {(m['title'] or '')[:48]}")
    if s["top_wallets"]:
        lines.append("\n🏆 *Top whales:*")
        for w in s["top_wallets"]:
            lines.append(f"  {fmt_money(w['vol'])} ({w['n']} trades) — "
                         f"{w['name'] or short(w['wallet'])}")
    text = "\n".join(lines)
    print("\n" + text + "\n")
    if args.send:
        chans = notifier.notify(text)
        print(f"→ sent to: {chans or 'no channels configured'}")
    conn.close()


def _evaluate(conn, t, args, follow, score_cache):
    """Return (reasons, score_or_None) for a trade given the active filters."""
    reasons = []

    # Final safety gate for every alert path.
    side = (t.get("side") or "").upper()
    if side == "BUY":
        if not _eligible_entry(t):
            return [], None
    elif side == "SELL":
        if not is_sports_trade(t):
            return [], None
    else:
        return [], None
    wallet = (t.get("proxyWallet") or "").lower()
    v = usd(t)
    if follow and wallet in follow:
        reasons.append("watchlist")
    if v >= args.min_usd:
        # whale by size; optionally also require smart-money
        if args.smart:
            score = score_cache.get(wallet)
            if score is None:
                cached = DB.get_score(conn, wallet)
                if cached:
                    score = cached
                else:
                    try:
                        score = score_wallet(wallet)
                        DB.save_score(conn, wallet, score)
                    except Exception:
                        score = None
                score_cache[wallet] = score
            if score and is_smart(score, args.min_pnl, args.min_winrate, args.min_closed):
                reasons.append("smart")
            elif not reasons:  # not watchlist, didn't pass smart -> skip
                return [], score
            return (reasons + (["whale"] if "smart" in reasons else [])), score
        else:
            reasons.append("whale")
    return reasons, score_cache.get(wallet)


def _spike_markets(conn, trades, args):
    """Detect markets with high recent volume. Returns {condition_id: volume}."""
    since = int(time.time()) - args.spike_window * 60
    markets = {t.get("conditionId") for t in trades if t.get("conditionId")}
    hits = {}
    for cid in markets:
        vol, n = DB.market_volume_since(conn, cid, since)
        if vol >= args.spike_usd and n >= args.spike_trades:
            hits[cid] = vol
    return hits


def _watchlist_set(conn):
    return {w["wallet"] for w in DB.watchlist_all(conn)}


def cmd_wallet(args):
    w = args.address
    val = fetch_value(w)
    positions = fetch_positions(w, limit=200)
    trades = [t for t in fetch_trades(limit=1000) if (t.get("proxyWallet") or "").lower() == w.lower()]
    open_pos = [p for p in positions if float(p.get("currentValue", 0)) > 0.01]
    total_pnl = sum(float(p.get("cashPnl", 0)) for p in positions)
    print(f"\n💼 WALLET {w}\n" + "-" * 88)
    print(f"Saldo posisi sekarang : {fmt_money(val)}")
    print(f"Total PnL (all-time)  : {fmt_money(total_pnl)}")
    print(f"Posisi terbuka        : {len(open_pos)}")
    print("\nTop posisi terbuka:")
    for p in sorted(open_pos, key=lambda x: float(x.get("currentValue", 0)), reverse=True)[:10]:
        print(f"  {fmt_money(float(p.get('currentValue',0))):>12}  PnL {fmt_money(float(p.get('cashPnl',0))):>12}"
              f"  {(p.get('outcome') or '?')[:8]:<8} | {(p.get('title') or '')[:46]}")
    print("\nTrade terakhir:")
    for t in sorted(trades, key=lambda x: x.get("timestamp", 0), reverse=True)[:10]:
        print(f"  [{ts_str(t.get('timestamp'))}] {trade_line(t)}")
    print()


def cmd_leaderboard(args):
    trades = fetch_trades(limit=args.lookback)
    agg = {}
    for t in trades:
        w = t.get("proxyWallet", "")
        a = agg.setdefault(w, {"name": name_of(t), "vol": 0.0, "n": 0})
        a["vol"] += usd(t); a["n"] += 1
    ranked = sorted(agg.items(), key=lambda kv: kv[1]["vol"], reverse=True)
    print(f"\n🏆 LEADERBOARD — dari {len(trades)} trade terakhir\n" + "-" * 80)
    print(f"{'#':>2}  {'Volume':>14}  {'Trades':>6}  {'Trader':<20}  Wallet")
    for i, (w, a) in enumerate(ranked[: args.top], 1):
        print(f"{i:>2}  {fmt_money(a['vol']):>14}  {a['n']:>6}  {a['name'][:20]:<20}  {short(w)}")
    print()


def cmd_score(args):
    s = score_wallet(args.address)
    print(f"\n🧠 SMART-MONEY SCORE — {args.address}\n" + "-" * 60)
    print(f"Realized PnL : {fmt_money(s['realized_pnl'])}")
    print(f"Win rate     : {s['winrate']*100:.1f}%  ({s['n_closed']} closed positions)")
    print(f"Open value   : {fmt_money(s['cur_value'])}")
    verdict = "✅ SMART MONEY" if is_smart(s, 1000, 0.5, 5) else "⚠️ belum lolos filter default"
    print(f"Verdict      : {verdict}\n")


def cmd_watchlist(args):
    conn = DB.connect(args.db)
    if args.action == "add":
        DB.watchlist_add(conn, args.address, args.label or "")
        print(f"⭐ Added {args.address} ({args.label or 'no label'})")
    elif args.action == "remove":
        DB.watchlist_remove(conn, args.address)
        print(f"🗑️  Removed {args.address}")
    elif args.action == "list":
        wl = DB.watchlist_all(conn)
        print(f"\n⭐ WATCHLIST ({len(wl)})\n" + "-" * 60)
        for w in wl:
            print(f"  {short(w['wallet'])}  {w['label']}")
        print()
    elif args.action == "pnl":
        wl = DB.watchlist_all(conn)
        print(f"\n⭐ WATCHLIST PnL ({len(wl)})\n" + "-" * 78)
        print(f"{'Wallet':<16}  {'Open val':>12}  {'Realized PnL':>14}  {'WinRate':>8}  Label")
        for w in wl:
            try:
                s = score_wallet(w["wallet"])
                print(f"{short(w['wallet']):<16}  {fmt_money(s['cur_value']):>12}  "
                      f"{fmt_money(s['realized_pnl']):>14}  {s['winrate']*100:>6.0f}%  {w['label']}")
            except Exception as e:
                print(f"{short(w['wallet']):<16}  (error: {e})")
        print()
    conn.close()


# ----------------------------- CLI -----------------------------
def _add_filter_args(p, default_min):
    p.add_argument("--min-usd", type=float, default=default_min)
    p.add_argument("--lookback", type=int, default=10000)
    p.add_argument("--smart", action="store_true", help="hanya alert wallet smart-money")
    p.add_argument("--min-pnl", type=float, default=1000)
    p.add_argument("--min-winrate", type=float, default=0.5)
    p.add_argument("--min-closed", type=int, default=5)
    p.add_argument("--db", type=str, default="tracker.db")


def main():
    p = argparse.ArgumentParser(description="Polymarket Whale Tracker v2 (read-only)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sc = sub.add_parser("scan"); sc.add_argument("--min-usd", type=float, default=1000)
    sc.add_argument("--lookback", type=int, default=500); sc.add_argument("--top", type=int, default=30)
    sc.set_defaults(func=cmd_scan)

    wt = sub.add_parser("watch"); _add_filter_args(wt, 5000)
    wt.add_argument("--interval", type=int, default=15); wt.add_argument("--follow", type=str, default="")
    wt.set_defaults(func=cmd_watch)

    pl = sub.add_parser("poll"); _add_filter_args(pl, 5000)
    pl.add_argument("--spike-usd", type=float, default=0, help="alert market kalau volume window >= ini")
    pl.add_argument("--spike-window", type=int, default=30, help="menit")
    pl.add_argument("--spike-trades", type=int, default=3)
    pl.add_argument("--consensus-wallets", type=int, default=0,
                    help="alert kalau >= N wallet beli sisi sama (0=off)")
    pl.add_argument("--consensus-window", type=int, default=60, help="menit")
    pl.add_argument("--consensus-min-usd", type=float, default=1000,
                    help="minimal USD per trade buat dihitung di consensus")
    pl.set_defaults(func=cmd_poll)

    cs = sub.add_parser("consensus")
    cs.add_argument("--wallets", type=int, default=3, help="minimal jumlah wallet")
    cs.add_argument("--min-usd", type=float, default=1000)
    cs.add_argument("--window", type=int, default=60, help="menit")
    cs.add_argument("--lookback", type=int, default=1000)
    cs.add_argument("--top", type=int, default=10)
    cs.add_argument("--db", type=str, default="tracker.db")
    cs.set_defaults(func=cmd_consensus)

    dg = sub.add_parser("digest")
    dg.add_argument("--hours", type=int, default=24)
    dg.add_argument("--send", action="store_true", help="kirim ke Telegram/Discord")
    dg.add_argument("--db", type=str, default="tracker.db")
    dg.set_defaults(func=cmd_digest)

    wl = sub.add_parser("wallet"); wl.add_argument("address"); wl.set_defaults(func=cmd_wallet)

    lb = sub.add_parser("leaderboard"); lb.add_argument("--lookback", type=int, default=1000)
    lb.add_argument("--top", type=int, default=20); lb.set_defaults(func=cmd_leaderboard)

    scr = sub.add_parser("score"); scr.add_argument("address"); scr.set_defaults(func=cmd_score)

    wlc = sub.add_parser("watchlist")
    wlc.add_argument("action", choices=["add", "remove", "list", "pnl"])
    wlc.add_argument("address", nargs="?", default="")
    wlc.add_argument("--label", type=str, default=""); wlc.add_argument("--db", type=str, default="tracker.db")
    wlc.set_defaults(func=cmd_watchlist)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
