"""SQLite history store for trades, alerts, wallet scores, and watchlist."""
import sqlite3
import time

DEFAULT_PATH = "tracker.db"


def connect(path=DEFAULT_PATH):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    _init(conn)
    return conn


def _init(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS trades (
            tx_key TEXT PRIMARY KEY,
            wallet TEXT, name TEXT, side TEXT,
            condition_id TEXT, asset TEXT,
            size REAL, price REAL, usd REAL,
            title TEXT, outcome TEXT, ts INTEGER,
            inserted_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts);
        CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(condition_id, ts);
        CREATE INDEX IF NOT EXISTS idx_trades_wallet ON trades(wallet, ts);
        CREATE INDEX IF NOT EXISTS idx_trades_accum ON trades(wallet, condition_id, outcome, side, price, ts);

        CREATE TABLE IF NOT EXISTS alerts (
            tx_key TEXT PRIMARY KEY,
            reason TEXT,
            alerted_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS wallet_scores (
            wallet TEXT PRIMARY KEY,
            realized_pnl REAL, winrate REAL, n_closed INTEGER,
            cur_value REAL, scored_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            wallet TEXT PRIMARY KEY,
            label TEXT,
            added_at INTEGER
        );
        """
    )
    conn.commit()


def insert_trade(conn, t, usd_val):
    try:
        conn.execute(
            """INSERT OR IGNORE INTO trades
               (tx_key,wallet,name,side,condition_id,asset,size,price,usd,title,outcome,ts,inserted_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                f"{t.get('transactionHash','')}:{t.get('asset','')}:{t.get('timestamp','')}",
                (t.get("proxyWallet") or "").lower(),
                t.get("name") or t.get("pseudonym") or "",
                t.get("side"), t.get("conditionId"), t.get("asset"),
                float(t.get("size", 0)), float(t.get("price", 0)), usd_val,
                t.get("title"), t.get("outcome"), int(t.get("timestamp", 0)),
                int(time.time()),
            ),
        )
        return conn.total_changes
    except Exception:
        return 0


def already_alerted(conn, tx_key):
    return conn.execute("SELECT 1 FROM alerts WHERE tx_key=?", (tx_key,)).fetchone() is not None


def mark_alerted(conn, tx_key, reason):
    conn.execute("INSERT OR IGNORE INTO alerts (tx_key,reason,alerted_at) VALUES (?,?,?)",
                 (tx_key, reason, int(time.time())))


def get_score(conn, wallet, max_age_sec=86400):
    row = conn.execute("SELECT * FROM wallet_scores WHERE wallet=?", (wallet.lower(),)).fetchone()
    if row and (int(time.time()) - row["scored_at"]) <= max_age_sec:
        return dict(row)
    return None


def save_score(conn, wallet, score):
    conn.execute(
        """INSERT OR REPLACE INTO wallet_scores
           (wallet,realized_pnl,winrate,n_closed,cur_value,scored_at) VALUES (?,?,?,?,?,?)""",
        (wallet.lower(), score["realized_pnl"], score["winrate"],
         score["n_closed"], score["cur_value"], int(time.time())),
    )


def market_volume_since(conn, condition_id, since_ts):
    row = conn.execute(
        "SELECT COALESCE(SUM(usd),0) v, COUNT(*) n FROM trades WHERE condition_id=? AND ts>=?",
        (condition_id, since_ts)).fetchone()
    return row["v"], row["n"]


# ---- watchlist ----
def watchlist_add(conn, wallet, label=""):
    conn.execute("INSERT OR REPLACE INTO watchlist (wallet,label,added_at) VALUES (?,?,?)",
                 (wallet.lower(), label, int(time.time())))
    conn.commit()


def watchlist_remove(conn, wallet):
    conn.execute("DELETE FROM watchlist WHERE wallet=?", (wallet.lower(),))
    conn.commit()


def watchlist_all(conn):
    return [dict(r) for r in conn.execute("SELECT * FROM watchlist ORDER BY added_at").fetchall()]


# ---- consensus (multiple wallets buying the same side) ----
def consensus_groups(conn, since_ts, min_usd, min_wallets):
    """Markets where >= min_wallets distinct wallets BOUGHT the same outcome."""
    rows = conn.execute(
        """SELECT condition_id, outcome, MAX(title) title,
                  COUNT(DISTINCT wallet) n_wallets, SUM(usd) vol
           FROM trades
           WHERE ts>=? AND usd>=? AND side='BUY'
           GROUP BY condition_id, outcome
           HAVING COUNT(DISTINCT wallet) >= ?
           ORDER BY vol DESC""",
        (since_ts, min_usd, min_wallets)).fetchall()
    return [dict(r) for r in rows]


def consensus_wallets(conn, condition_id, outcome, since_ts, min_usd, limit=5):
    rows = conn.execute(
        """SELECT wallet, MAX(name) name, SUM(usd) vol
           FROM trades
           WHERE condition_id=? AND outcome=? AND ts>=? AND usd>=? AND side='BUY'
           GROUP BY wallet ORDER BY vol DESC LIMIT ?""",
        (condition_id, outcome, since_ts, min_usd, limit)).fetchall()
    return [dict(r) for r in rows]


# ---- digest ----
def digest_stats(conn, since_ts):
    total = conn.execute(
        "SELECT COALESCE(SUM(usd),0) v, COUNT(*) n, COUNT(DISTINCT wallet) w FROM trades WHERE ts>=?",
        (since_ts,)).fetchone()
    top_trades = conn.execute(
        "SELECT * FROM trades WHERE ts>=? ORDER BY usd DESC LIMIT 5", (since_ts,)).fetchall()
    top_markets = conn.execute(
        """SELECT condition_id, MAX(title) title, SUM(usd) vol, COUNT(*) n
           FROM trades WHERE ts>=? GROUP BY condition_id ORDER BY vol DESC LIMIT 5""",
        (since_ts,)).fetchall()
    top_wallets = conn.execute(
        """SELECT wallet, MAX(name) name, SUM(usd) vol, COUNT(*) n
           FROM trades WHERE ts>=? GROUP BY wallet ORDER BY vol DESC LIMIT 5""",
        (since_ts,)).fetchall()
    n_alerts = conn.execute(
        "SELECT COUNT(*) c FROM alerts WHERE alerted_at>=?", (since_ts,)).fetchone()["c"]
    return {
        "volume": total["v"], "n_trades": total["n"], "n_wallets": total["w"],
        "top_trades": [dict(r) for r in top_trades],
        "top_markets": [dict(r) for r in top_markets],
        "top_wallets": [dict(r) for r in top_wallets],
        "n_alerts": n_alerts,
    }
