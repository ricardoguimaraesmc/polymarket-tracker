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
from polymarket_api import fetch_trades, fetch_positions, fetch_value, usd, trade_key
from smartmoney import score_wallet, is_smart


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


def alert_text(t, reasons, score=None):
    arrow = "🟢 *BUY*" if t.get("side") == "BUY" else "🔴 *SELL*"
    tags = " ".join({"whale": "🐋WHALE", "smart": "🧠SMART-MONEY",
                     "watchlist": "⭐WATCHLIST", "spike": "📈SPIKE",
                     "consensus": "🎯CONSENSUS"}.get(r, r) for r in reasons)
    lines = [
        f"{tags}",
        f"{arrow} {fmt_money(usd(t))}  @ {float(t.get('price',0)):.3f} ({float(t.get('price',0))*100:.0f}%)",
        f"*{t.get('title','')}*",
        f"Outcome: *{t.get('outcome','?')}*  |  Trader: `{name_of(t)}`",
    ]
    if score:
        lines.append(f"Trader stats: PnL {fmt_money(score['realized_pnl'])} | "
                     f"winrate {score['winrate']*100:.0f}% ({score['n_closed']} closed)")
    wallet = t.get("proxyWallet", "")
    lines.append(f"https://polymarket.com/profile/{wallet}")
    return "\n".join(lines)


# ----------------------------- commands -----------------------------
def cmd_scan(args):
    trades = fetch_trades(limit=args.lookback)
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
                trades = fetch_trades(limit=args.lookback)
            except Exception as e:
                print(f"⚠️ fetch error: {e}", file=sys.stderr)
                time.sleep(args.interval)
                continue
            for t in sorted(trades, key=lambda x: x.get("timestamp", 0)):
                k = trade_key(t)
                if k in seen:
                    continue
                seen.add(k)
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
    """Monitora BUY acumulado e reducao de posicao da mesma carteira."""
    conn = DB.connect(args.db)

    # Aproximadamente R$ 990 mil
    ACCUMULATED_MIN_USD = 190_000

    # ---------------------------------------------------------
    # Busca trades recentes
    # ---------------------------------------------------------
    trades = fetch_trades(limit=args.lookback)

    # Guarda somente trades que realmente entraram no banco agora.
    # Isso evita tratar SELL antigo como uma nova retirada.
    new_trades = []

    for t in trades:
        tx_key = (
            f"{t.get('transactionHash', '')}:"
            f"{t.get('asset', '')}:"
            f"{t.get('timestamp', '')}"
        )

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
    # 1) HISTORICO COMPLETO
    #
    # Agrupa:
    # mesma carteira + mesmo mercado + mesma selecao
    # =========================================================
    groups = conn.execute(
        """
        SELECT
            wallet,
            condition_id,
            outcome,
            COALESCE(
                SUM(CASE WHEN side='BUY' THEN usd ELSE 0 END), 0
            ) AS buy_total,
            COALESCE(
                SUM(CASE WHEN side='SELL' THEN usd ELSE 0 END), 0
            ) AS sell_total,
            COUNT(
                CASE WHEN side='BUY' THEN 1 END
            ) AS buy_count
        FROM trades
        WHERE wallet IS NOT NULL
          AND wallet != ''
          AND condition_id IS NOT NULL
          AND condition_id != ''
          AND outcome IS NOT NULL
          AND outcome != ''
        GROUP BY wallet, condition_id, outcome
        HAVING buy_total >= ?
        """,
        (ACCUMULATED_MIN_USD,),
    ).fetchall()

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

        alert_key = (
            f"accumulated:{wallet}:"
            f"{condition_id}:{outcome}"
        )

        # Ja avisou anteriormente.
        if DB.already_alerted(conn, alert_key):
            continue

        # Descobre exatamente quando cruzou US$ 190 mil.
        buy_rows = conn.execute(
            """
            SELECT ts, usd
            FROM trades
            WHERE wallet=?
              AND condition_id=?
              AND outcome=?
              AND side='BUY'
            ORDER BY ts ASC
            """,
            (wallet, condition_id, outcome),
        ).fetchall()

        running_buy = 0.0
        threshold_ts = None

        for b in buy_rows:
            running_buy += float(b["usd"] or 0)

            if running_buy >= ACCUMULATED_MIN_USD:
                threshold_ts = int(b["ts"])
                break

        if threshold_ts is None:
            continue

        latest = conn.execute(
            """
            SELECT title
            FROM trades
            WHERE wallet=?
              AND condition_id=?
              AND outcome=?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (wallet, condition_id, outcome),
        ).fetchone()

        title = (
            latest["title"]
            if latest and latest["title"]
            else "Mercado Polymarket"
        )

        txt = (
            f"🐋 *APOSTA ACUMULADA DETECTADA*\n\n"
            f"🎯 *Mercado:* {title}\n"
            f"📌 *Seleção:* {outcome}\n"
            f"💰 *BUY acumulado:* ${buy_total:,.0f}\n"
            f"🧾 *Entradas BUY:* {buy_count}\n"
            f"🔴 *SELL acumulado:* ${sell_total:,.0f}\n"
            f"📊 *Posição estimada:* "
            f"${buy_total - sell_total:,.0f}\n"
            f"👛 *Carteira:* `{wallet[:12]}...`\n\n"
            f"🚨 *A carteira cruzou "
            f"${ACCUMULATED_MIN_USD:,.0f} em BUY nessa seleção.*"
        )

        chans = notifier.notify(txt)

        DB.mark_alerted(
            conn,
            alert_key,
            "accumulated",
        )

        new_alerts += 1

        print(
            f"ALERT [accumulated] "
            f"{title[:50]} | {outcome} | "
            f"BUY ${buy_total:,.0f} -> "
            f"{chans or 'terminal'}"
        )

    # =========================================================
    # 3) NOVOS SELL / REDUCAO DE POSICAO
    #
    # SOMENTE trades que entraram neste poll.
    # =========================================================
    for t in sorted(
        new_trades,
        key=lambda x: x.get("timestamp", 0),
    ):

        side = (t.get("side") or "").upper()

        if side != "SELL":
            continue

        wallet = (
            t.get("proxyWallet")
            or ""
        ).lower()

        condition_id = t.get("conditionId") or ""
        outcome = t.get("outcome") or ""
        sell_ts = int(t.get("timestamp", 0))

        if not wallet or not condition_id or not outcome:
            continue

        # -----------------------------------------------------
        # Descobre quando essa carteira atingiu US$ 190 mil.
        # -----------------------------------------------------
        buy_rows = conn.execute(
            """
            SELECT ts, usd
            FROM trades
            WHERE wallet=?
              AND condition_id=?
              AND outcome=?
              AND side='BUY'
            ORDER BY ts ASC
            """,
            (wallet, condition_id, outcome),
        ).fetchall()

        running_buy = 0.0
        threshold_ts = None

        for b in buy_rows:
            running_buy += float(b["usd"] or 0)

            if running_buy >= ACCUMULATED_MIN_USD:
                threshold_ts = int(b["ts"])
                break

        # Nunca chegou a US$ 190 mil.
        if threshold_ts is None:
            continue

        # SELL precisa acontecer DEPOIS do cruzamento.
        if sell_ts <= threshold_ts:
            continue

        sell_key = f"reverse:{trade_key(t)}"

        if DB.already_alerted(conn, sell_key):
            continue

        # -----------------------------------------------------
        # Calcula a posicao no momento exato do SELL.
        # -----------------------------------------------------
        totals = conn.execute(
            """
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN side='BUY' AND ts<=?
                            THEN usd
                            ELSE 0
                        END
                    ), 0
                ) AS bought,

                COALESCE(
                    SUM(
                        CASE
                            WHEN side='SELL' AND ts<=?
                            THEN usd
                            ELSE 0
                        END
                    ), 0
                ) AS sold

            FROM trades
            WHERE wallet=?
              AND condition_id=?
              AND outcome=?
            """,
            (
                sell_ts,
                sell_ts,
                wallet,
                condition_id,
                outcome,
            ),
        ).fetchone()

        bought = float(totals["bought"] or 0)
        sold = float(totals["sold"] or 0)
        position = bought - sold

        title = (
            t.get("title")
            or "Mercado Polymarket"
        )

        txt = (
            f"🔴 *REDUÇÃO DE POSIÇÃO DETECTADA*\n\n"
            f"🎯 *Mercado:* {title}\n"
            f"📌 *Seleção:* {outcome}\n"
            f"👛 *Carteira:* `{wallet[:12]}...`\n\n"
            f"💰 *BUY acumulado:* ${bought:,.0f}\n"
            f"🔴 *SELL acumulado:* ${sold:,.0f}\n"
            f"📊 *Posição estimada:* "
            f"${position:,.0f}\n"
            f"🔻 *Novo SELL:* ${usd(t):,.0f}\n\n"
            f"⚠️ *Essa carteira havia cruzado "
            f"${ACCUMULATED_MIN_USD:,.0f} em BUY "
            f"e agora está reduzindo a posição.*"
        )

        chans = notifier.notify(txt)

        DB.mark_alerted(
            conn,
            sell_key,
            "reverse",
        )

        new_alerts += 1

        print(
            f"ALERT [reverse] "
            f"{title[:50]} | {outcome} | "
            f"SELL ${usd(t):,.0f} | "
            f"position ${position:,.0f} -> "
            f"{chans or 'terminal'}"
        )

    conn.commit()
    conn.close()

    print(
        f"\n✅ poll done. "
        f"{len(trades)} trades scanned, "
        f"{new_alerts} new alerts sent."
    )

def cmd_consensus(args):
    """Find markets where several whales bought the same outcome recently."""
    conn = DB.connect(args.db)
    trades = fetch_trades(limit=args.lookback)
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
    p.add_argument("--lookback", type=int, default=500)
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
