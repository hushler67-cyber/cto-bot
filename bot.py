"""
CTO Bot
Finds Solana memecoins that were once big, got rugged, and still have a
community you could take over. Sends an alert to Telegram for each one.

Free data sources only:
  - DexScreener   : price, market cap, liquidity, volume, socials, CTO list
  - GeckoTerminal : daily candles -> all-time high
  - Helius (free) : holder count, top 10 %, mint/freeze authority

How it works (one "cycle" every SCAN_INTERVAL_SECONDS):
  1. discover()     - collect hot/boosted/trending Solana coins + your WATCHLIST
                      and remember them in SQLite.
  2. check_tokens() - re-check remembered coins. While a coin is still big,
                      the bot snapshots its holder count once a day, so it
                      knows the peak. When a coin looks rugged it gets the
                      full check.
  3. send_telegram()- "READY TO CTO" alert, once per coin.
"""

import html
import logging
import os
import sqlite3
import time

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------- settings
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
SOLANA_RPC = (
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    if HELIUS_API_KEY
    else os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com")
)

# On Railway, mount a Volume at /data so the database survives redeploys.
DB_PATH = os.getenv("DB_PATH", "/data/cto.db" if os.path.isdir("/data") else "cto.db")

# Rug rules (change them in Railway Variables, no code change needed)
MIN_ATH_MCAP = float(os.getenv("MIN_ATH_MCAP", "1000000"))      # was "successful"
MIN_DROP_PCT = float(os.getenv("MIN_DROP_PCT", "90"))           # down this much from ATH
MAX_VOLUME_24H = float(os.getenv("MAX_VOLUME_24H", "20000"))    # trading is dead
MIN_PEAK_HOLDERS = int(os.getenv("MIN_PEAK_HOLDERS", "500"))    # had a real community
REQUIRE_SOCIALS = os.getenv("REQUIRE_SOCIALS", "true").lower() == "true"

# Holder counting: 1 page = 1000 token accounts. 10 pages = counts up to 10,000.
MAX_HOLDER_PAGES = int(os.getenv("MAX_HOLDER_PAGES", "10"))

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL_SECONDS", "600"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "60"))
WATCHLIST = [a.strip() for a in os.getenv("WATCHLIST", "").split(",") if a.strip()]

DEX = "https://api.dexscreener.com"
GT = "https://api.geckoterminal.com/api/v2"
GT_PAUSE = 2.5      # GeckoTerminal free tier is ~30 calls/minute
DAS_PAUSE = 0.6     # Helius free plan allows 2 DAS calls/second
DAY = 86400

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cto-bot")

session = requests.Session()
session.headers.update({"accept": "application/json", "User-Agent": "cto-bot/1.0"})


# ---------------------------------------------------------------- helpers
def get_json(url, params=None, pause=0.0):
    """GET a URL and return JSON. Retries on rate limits. Returns None on failure."""
    try:
        for attempt in range(3):
            try:
                r = session.get(url, params=params, timeout=20)
                if r.status_code == 429:
                    wait = 10 * (attempt + 1)
                    log.warning("Rate limited by %s, waiting %ss", url, wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                log.warning("GET failed %s: %s", url, e)
                time.sleep(3)
        return None
    finally:
        if pause:
            time.sleep(pause)


def rpc(method, params):
    """Call the Solana RPC (Helius if you set HELIUS_API_KEY). Returns result or None."""
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    for attempt in range(3):
        try:
            r = session.post(SOLANA_RPC, json=body, timeout=30)
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                log.warning("RPC %s error: %s", method, data["error"])
                return None
            return data.get("result")
        except requests.RequestException as e:
            log.warning("RPC %s failed: %s", method, e)
            time.sleep(3)
    return None


def money(n):
    n = float(n or 0)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= div:
            return f"${n / div:.2f}{suffix}"
    return f"${n:,.2f}"


# ---------------------------------------------------------------- database
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE IF NOT EXISTS tokens (
            address        TEXT PRIMARY KEY,
            name           TEXT,
            symbol         TEXT,
            pair_address   TEXT,
            peak_mcap_seen REAL DEFAULT 0,
            ath_mcap       REAL DEFAULT 0,
            ath_checked_at INTEGER DEFAULT 0,
            last_checked   INTEGER DEFAULT 0,
            alerted        INTEGER DEFAULT 0,
            added_at       INTEGER
        )"""
    )
    # New columns for holder tracking (safe to run on an old database)
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(tokens)")]
    if "peak_holders" not in cols:
        conn.execute("ALTER TABLE tokens ADD COLUMN peak_holders INTEGER DEFAULT 0")
    if "holders_checked_at" not in cols:
        conn.execute("ALTER TABLE tokens ADD COLUMN holders_checked_at INTEGER DEFAULT 0")
    conn.execute("CREATE TABLE IF NOT EXISTS cto_done (address TEXT PRIMARY KEY)")
    conn.commit()
    return conn


def add_tokens(conn, addresses):
    now = int(time.time())
    new = 0
    for a in addresses:
        cur = conn.execute(
            "INSERT OR IGNORE INTO tokens (address, added_at) VALUES (?, ?)", (a, now)
        )
        new += cur.rowcount
    conn.commit()
    return new


# ---------------------------------------------------------------- 1. discovery
def discover(conn):
    """Collect Solana coins worth remembering. Hot coins today are tomorrow's rugs."""
    found = set(WATCHLIST)

    for path in ("/token-profiles/latest/v1", "/token-boosts/latest/v1", "/token-boosts/top/v1"):
        data = get_json(DEX + path) or []
        if isinstance(data, dict):
            data = [data]
        for item in data:
            if item.get("chainId") == "solana" and item.get("tokenAddress"):
                found.add(item["tokenAddress"])

    for page in (1, 2, 3):
        data = get_json(f"{GT}/networks/solana/trending_pools", params={"page": page}, pause=GT_PAUSE)
        for pool in (data or {}).get("data", []):
            base_id = (
                pool.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "")
            )
            if base_id.startswith("solana_"):
                found.add(base_id[len("solana_"):])

    # Coins someone already took over on DexScreener -> skip them
    ctos = get_json(DEX + "/community-takeovers/latest/v1") or []
    if isinstance(ctos, dict):
        ctos = [ctos]
    for item in ctos:
        if item.get("chainId") == "solana" and item.get("tokenAddress"):
            conn.execute("INSERT OR IGNORE INTO cto_done (address) VALUES (?)", (item["tokenAddress"],))
    conn.commit()

    new = add_tokens(conn, found)
    total = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
    log.info("Discovery: %s seen, %s new, %s tracked in total", len(found), new, total)


# ---------------------------------------------------------------- 2. data sources
def dex_best_pairs(addresses):
    """DexScreener allows 30 tokens per call. Keep the most liquid pair per token."""
    best = {}
    for i in range(0, len(addresses), 30):
        chunk = addresses[i:i + 30]
        pairs = get_json(f"{DEX}/tokens/v1/solana/{','.join(chunk)}") or []
        for p in pairs:
            addr = (p.get("baseToken") or {}).get("address")
            if addr not in chunk:
                continue
            liq = (p.get("liquidity") or {}).get("usd") or 0
            old_liq = ((best.get(addr) or {}).get("liquidity") or {}).get("usd") or 0
            if addr not in best or liq > old_liq:
                best[addr] = p
    return best


def gecko_ath_price(pair_address):
    """Highest price ever, from daily candles (up to 1000 days)."""
    data = get_json(
        f"{GT}/networks/solana/pools/{pair_address}/ohlcv/day",
        params={"limit": 1000, "currency": "usd", "token": "base"},
        pause=GT_PAUSE,
    )
    candles = (((data or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
    if not candles:
        return None
    return max(float(c[2]) for c in candles)  # c = [time, open, high, low, close, volume]


def count_holders(mint):
    """
    Count wallets holding the coin (Helius DAS getTokenAccounts, 1000 per page).
    Returns (count, capped). capped=True means there are more than we counted.
    """
    if not HELIUS_API_KEY:
        return None, False
    owners = set()
    for page in range(1, MAX_HOLDER_PAGES + 1):
        result = rpc(
            "getTokenAccounts",
            {"mint": mint, "page": page, "limit": 1000, "options": {"showZeroBalance": False}},
        )
        time.sleep(DAS_PAUSE)
        if result is None:
            return (len(owners) or None), False
        accounts = result.get("token_accounts") or []
        for acc in accounts:
            if int(acc.get("amount") or 0) > 0 and acc.get("owner"):
                owners.add(acc["owner"])
        if len(accounts) < 1000:
            return len(owners), False
    return len(owners), True


def top10_percent(mint):
    """% of supply in the 10 biggest token accounts (includes the LP pool account)."""
    largest = rpc("getTokenLargestAccounts", [mint])
    supply = rpc("getTokenSupply", [mint])
    try:
        total = float(supply["value"]["uiAmount"] or 0)
        top = sum(float(a.get("uiAmount") or 0) for a in largest["value"][:10])
        return (top / total * 100) if total else None
    except (TypeError, KeyError):
        return None


def mint_authorities(mint):
    """If mint or freeze authority is still active, the dev can still mess with the coin."""
    result = rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    try:
        info = result["value"]["data"]["parsed"]["info"]
        return {"mint": info.get("mintAuthority"), "freeze": info.get("freezeAuthority")}
    except (TypeError, KeyError):
        return None


def community_links(pair):
    info = pair.get("info") or {}
    links = []
    for s in info.get("socials") or []:
        if s.get("url"):
            links.append(((s.get("type") or "social").capitalize(), s["url"]))
    for w in info.get("websites") or []:
        if w.get("url"):
            links.append(("Website", w["url"]))
    return links


# ---------------------------------------------------------------- 3. alert
def send_telegram(text):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        r = session.post(
            url,
            json={
                "chat_id": TG_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        if not r.ok:
            log.error("Telegram error %s: %s", r.status_code, r.text)
        return r.ok
    except requests.RequestException as e:
        log.error("Telegram send failed: %s", e)
        return False


def fmt_count(n, capped):
    if n is None:
        return "?"
    return f"{n:,}+" if capped else f"{n:,}"


def build_alert(pair, ath_mcap, drop, holders, auth, top10, socials):
    t = pair["baseToken"]
    mcap = pair.get("marketCap") or pair.get("fdv") or 0
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol = (pair.get("volume") or {}).get("h24") or 0
    tx = (pair.get("txns") or {}).get("h24") or {}

    lines = [
        "🚨 <b>READY TO CTO</b>",
        "",
        f"<b>{html.escape(str(t.get('name')))} (${html.escape(str(t.get('symbol')))})</b>",
        f"Contract: <code>{t['address']}</code>",
        "",
        f"ATH market cap: <b>{money(ath_mcap)}</b>",
        f"Now: {money(mcap)} (down {drop:.1f}%)",
        f"Price: ${pair.get('priceUsd')}",
        f"Liquidity: {money(liq)}",
        f"Volume 24h: {money(vol)}",
        f"Txns 24h: {tx.get('buys', 0)} buys / {tx.get('sells', 0)} sells",
    ]

    if holders:
        peak = holders["peak"]
        lines.append(
            f"Holders now: {fmt_count(holders['current'], holders['capped'])}"
            f" | Peak seen: {fmt_count(peak, False) if peak else '?'}"
        )
    if top10 is not None:
        lines.append(f"Top 10 accounts hold: {top10:.1f}% (incl. LP)")

    if auth:
        lines.append("Mint authority: " + ("revoked ✅" if not auth["mint"] else "ACTIVE ⚠️"))
        lines.append("Freeze authority: " + ("revoked ✅" if not auth["freeze"] else "ACTIVE ⚠️"))

    lines += ["", "<b>Community</b>"]
    if socials:
        for label, url in socials:
            lines.append(f"• {html.escape(label)}: {html.escape(url)}")
    else:
        lines.append("• none listed")

    lines += ["", f"Chart: {pair.get('url')}"]
    return "\n".join(lines)


# ---------------------------------------------------------------- 2+3. the check
def check_tokens(conn):
    rows = conn.execute(
        """SELECT * FROM tokens
           WHERE alerted = 0 AND address NOT IN (SELECT address FROM cto_done)
           ORDER BY last_checked ASC LIMIT ?""",
        (BATCH_SIZE,),
    ).fetchall()
    if not rows:
        return

    pairs = dex_best_pairs([r["address"] for r in rows])
    now = int(time.time())
    alerts = 0

    for row in rows:
        addr = row["address"]
        conn.execute("UPDATE tokens SET last_checked = ? WHERE address = ?", (now, addr))
        conn.commit()

        p = pairs.get(addr)
        if not p:
            continue

        price = float(p.get("priceUsd") or 0)
        mcap = float(p.get("marketCap") or p.get("fdv") or 0)
        vol24 = float((p.get("volume") or {}).get("h24") or 0)

        peak_seen = max(row["peak_mcap_seen"] or 0, mcap)
        ath_mcap = row["ath_mcap"] or 0
        ath_checked = row["ath_checked_at"] or 0
        peak_holders = row["peak_holders"] or 0
        holders_checked = row["holders_checked_at"] or 0

        # Refresh the ATH from GeckoTerminal once a day per coin
        if price > 0 and mcap > 0 and now - ath_checked > DAY:
            ath_price = gecko_ath_price(p["pairAddress"])
            if ath_price:
                supply = mcap / price
                ath_mcap = max(ath_mcap, ath_price * supply)
            ath_checked = now
        ath_mcap = max(ath_mcap, peak_seen)

        # While a coin is still big, snapshot its holders once a day -> builds the peak
        if HELIUS_API_KEY and mcap >= MIN_ATH_MCAP and now - holders_checked > DAY:
            n, _ = count_holders(addr)
            if n:
                peak_holders = max(peak_holders, n)
            holders_checked = now

        conn.execute(
            """UPDATE tokens SET name = ?, symbol = ?, pair_address = ?,
               peak_mcap_seen = ?, ath_mcap = ?, ath_checked_at = ?,
               peak_holders = ?, holders_checked_at = ? WHERE address = ?""",
            (
                p["baseToken"].get("name"),
                p["baseToken"].get("symbol"),
                p.get("pairAddress"),
                peak_seen,
                ath_mcap,
                ath_checked,
                peak_holders,
                holders_checked,
                addr,
            ),
        )
        conn.commit()

        # --- rug rules (cheap checks first) ---
        if ath_mcap < MIN_ATH_MCAP or mcap <= 0:
            continue
        drop = (1 - mcap / ath_mcap) * 100
        if drop < MIN_DROP_PCT or vol24 > MAX_VOLUME_24H:
            continue
        socials = community_links(p)
        if REQUIRE_SOCIALS and not socials:
            continue

        # --- Helius checks only for real candidates ---
        holders = None
        if HELIUS_API_KEY:
            current, capped = count_holders(addr)
            peak = max(peak_holders, current or 0) or None
            holders = {"current": current, "capped": capped, "peak": peak}
            if peak:
                conn.execute("UPDATE tokens SET peak_holders = ? WHERE address = ?", (peak, addr))
                conn.commit()
            if peak is not None and peak < MIN_PEAK_HOLDERS:
                continue
        top10 = top10_percent(addr)
        auth = mint_authorities(addr)

        msg = build_alert(p, ath_mcap, drop, holders, auth, top10, socials)
        if send_telegram(msg):
            conn.execute("UPDATE tokens SET alerted = 1 WHERE address = ?", (addr,))
            conn.commit()
            alerts += 1
            log.info("Alert sent: %s", p["baseToken"].get("symbol"))

    log.info("Checked %s coins, %s alerts", len(rows), alerts)


# ---------------------------------------------------------------- main loop
def main():
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        raise SystemExit("Set TG_BOT_TOKEN and TG_CHAT_ID first.")
    if not HELIUS_API_KEY:
        log.warning("No HELIUS_API_KEY - alerts will have no holder numbers.")

    conn = db_connect()
    log.info("Database: %s", DB_PATH)
    send_telegram("✅ CTO bot started. Watching Solana for rugged coins with a community.")

    while True:
        started = time.time()
        try:
            discover(conn)
            check_tokens(conn)
        except Exception:
            log.exception("Cycle failed, will retry next round")
        time.sleep(max(30, SCAN_INTERVAL - (time.time() - started)))


if __name__ == "__main__":
    main()