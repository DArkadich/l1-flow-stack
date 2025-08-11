import os, time, math, sqlite3, datetime as dt
from typing import List, Dict, Any, Optional
import ccxt
from pydantic import BaseModel, Field, field_validator
from telegram import Bot

DB_PATH = "/app/shared/ledger.db"

# ---------- Утилиты ----------
def sfloat(x: Any, default: float = 0.0) -> float:
    try:
        if x is None: return default
        return float(x)
    except Exception:
        return default

def now(): return dt.datetime.utcnow()
def now_s(): return now().strftime("%Y-%m-%d %H:%M:%S")
def daily_key(): return now().strftime("%Y-%m-%d")

# ---------- Конфиг ----------
class Cfg(BaseModel):
    key: str = Field(..., alias="BYBIT_API_KEY")
    sec: str = Field(..., alias="BYBIT_API_SECRET")
    acct: str = Field(..., alias="BYBIT_ACCOUNT_TYPE")
    symbols: List[str] = Field(..., alias="L1_SYMBOLS")
    fr_thr: float = Field(..., alias="L1_FUNDING_THRESHOLD_8H")
    max_alloc: float = Field(..., alias="L1_MAX_ALLOC_PCT")
    lev: int = Field(..., alias="L1_PERP_LEVERAGE")
    min_free: float = Field(..., alias="L1_MIN_FREE_BALANCE_USDT")
    poll: int = Field(..., alias="L1_POLL_INTERVAL_SEC")
    dd_day: float = Field(..., alias="L1_MAX_DAILY_DD_PCT")
    start_base: float = Field(..., alias="L1_START_BASE_USDT")
    pnl_thr_to_l2: float = Field(..., alias="L1_PNL_THRESHOLD_TO_L2")
    pnl_export_share: float = Field(..., alias="L1_PNL_EXPORT_SHARE")
    tg_token: str = Field(..., alias="TG_BOT_TOKEN")
    tg_chat: str = Field(..., alias="TG_CHAT_ID")

    @field_validator("symbols", mode="before")
    @classmethod
    def parse_symbols(cls, v):
        return [s.strip() for s in str(v).split(",") if s.strip()]

cfg = Cfg(**os.environ)

# ---------- Telegram ----------
bot = Bot(token=cfg.tg_token)
def tg(msg: str):
    try:
        bot.send_message(chat_id=cfg.tg_chat, text=msg[:4000], disable_web_page_preview=True)
    except Exception as e:
        print("TG error:", e)

# ---------- Биржа ----------
ex = ccxt.bybit({
    "apiKey": cfg.key,
    "secret": cfg.sec,
    "enableRateLimit": True,
    "options": { "defaultType": "unified" },
})

# ---------- SQLite ----------
def sql_conn():
    os.makedirs("/app/shared", exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT);""")
    con.execute("""CREATE TABLE IF NOT EXISTS trades(
        ts TEXT, sym TEXT, action TEXT, base REAL, quote REAL, info TEXT);""")
    con.execute("""CREATE TABLE IF NOT EXISTS daily_pnl(
        d TEXT PRIMARY KEY, pnl REAL);""")
    con.execute("""CREATE TABLE IF NOT EXISTS transfers(
        ts TEXT, direction TEXT, amount REAL, status TEXT, info TEXT);""")
    con.commit()
    return con

def sget(con, k, default=""):
    cur = con.execute("SELECT v FROM state WHERE k=?", (k,))
    r = cur.fetchone()
    return r[0] if r else default

def sset(con, k, v):
    con.execute("INSERT OR REPLACE INTO state(k,v) VALUES(?,?)", (k, str(v))); con.commit()

# ---------- Безопасные обёртки к API ----------
def fetch_balance_safe() -> Dict[str, Dict[str, float]]:
    try:
        bal = ex.fetch_balance(params={"type":"unified"}) or {}
        # нормализуем
        total = {k: sfloat(v, 0.0) for k, v in (bal.get("total") or {}).items()}
        free  = {k: sfloat(v, 0.0) for k, v in (bal.get("free")  or {}).items()}
        used  = {k: sfloat(v, 0.0) for k, v in (bal.get("used")  or {}).items()}
        return {"total": total, "free": free, "used": used}
    except Exception as e:
        print("fetch_balance_safe error:", e)
        return {"total": {}, "free": {}, "used": {}}

def total_equity() -> float:
    bal = fetch_balance_safe()
    return sfloat(bal["total"].get("USDT"), 0.0)

def free_equity() -> float:
    bal = fetch_balance_safe()
    return sfloat(bal["free"].get("USDT"), 0.0)

def mark(sym: str) -> float:
    # пытаемся взять last; если None — mid(bid,ask); если и это None — 0.0
    try:
        t = ex.fetch_ticker(sym) or {}
        last = sfloat(t.get("last"), 0.0)
        if last > 0: return last
        bid = sfloat(t.get("bid"), 0.0)
        ask = sfloat(t.get("ask"), 0.0)
        if bid > 0 and ask > 0: return (bid + ask) / 2.0
        # запасной вариант: глубина книги
        ob = ex.fetch_order_book(sym)
        best_bid = sfloat(ob["bids"][0][0], 0.0) if ob.get("bids") else 0.0
        best_ask = sfloat(ob["asks"][0][0], 0.0) if ob.get("asks") else 0.0
        if best_bid > 0 and best_ask > 0: return (best_bid + best_ask) / 2.0
        return 0.0
    except Exception as e:
        print("mark error:", e)
        return 0.0

def funding_8h(sym: str) -> float:
    # безопасно парсим funding; при ошибке — 0.0
    try:
        mkt = ex.market(sym)
        inst = mkt["id"]
        fr = ex.public_get_v5_market_funding_rate({"symbol": inst})
        lst = (((fr or {}).get("result") or {}).get("list") or [])
        if not lst:
            return 0.0
        rate = sfloat(lst[0].get("fundingRate"), 0.0)
        return rate
    except Exception as e:
        print("funding_8h error:", e)
        return 0.0

def set_leverage(sym: str, lev: int):
    try:
        mkt = ex.market(sym)
        ex.private_post_v5_position_set_leverage({
            "category":"linear","symbol":mkt["id"],
            "buyLeverage": str(lev), "sellLeverage": str(lev),
        })
    except Exception as e:
        print("set_leverage error:", e)

def positions(sym: str) -> Dict[str, float]:
    base = sym.split("/")[0]
    bal = fetch_balance_safe()
    spot = sfloat(bal["total"].get(base), 0.0)
    perp = 0.0
    try:
        mkt = ex.market(sym)
        pos = ex.private_get_v5_position_list({"category":"linear","symbol":mkt["id"]})
        lst = ((pos or {}).get("result") or {}).get("list") or []
        for p in lst:
            side = (p.get("side") or "").lower()
            sz = sfloat(p.get("size"), 0.0)
            if side == "buy":  perp += sz
            elif side == "sell": perp -= sz
    except Exception as e:
        print("positions error:", e)
    return {"spot": spot, "perp": perp}

def order_spot_buy(sym: str, quote_usdt: float):
    px = mark(sym)
    if px <= 0: raise RuntimeError(f"mark price unavailable for {sym}")
    base = round((quote_usdt / px) * 0.998, 6)  # запас на комиссии
    o = ex.create_order(sym, type="market", side="buy", amount=base)
    return base, o

def order_perp_sell(sym: str, base: float):
    set_leverage(sym, cfg.lev)
    return ex.create_order(sym, type="market", side="sell", amount=base, params={"reduceOnly": False})

def order_close_pair(sym: str):
    pos = positions(sym)
    try:
        if abs(pos["perp"]) > 1e-6:
            ex.create_order(sym, type="market",
                            side=("buy" if pos["perp"] < 0 else "sell"),
                            amount=abs(pos["perp"]), params={"reduceOnly": True})
        if pos["spot"] > 1e-6:
            ex.create_order(sym, type="market", side="sell", amount=pos["spot"])
    except Exception as e:
        print("order_close_pair error:", e)

def in_funding_window() -> bool:
    # за 2–3 минуты до часа (00/08/16 UTC)
    t = now()
    return t.minute in (57, 58)

def update_daily_pnl(con, prev_equity: float, new_equity: float):
    d = daily_key()
    pnl = (new_equity - prev_equity) if prev_equity > 0 else 0.0
    cur = con.execute("SELECT pnl FROM daily_pnl WHERE d=?", (d,)).fetchone()
    if cur:
        con.execute("UPDATE daily_pnl SET pnl=? WHERE d=?", (pnl, d))
    else:
        con.execute("INSERT INTO daily_pnl(d,pnl) VALUES(?,?)", (d, pnl))
    con.commit()
    return pnl

def daily_drawdown_exceeded(con, start_e: float):
    d = daily_key()
    cur = con.execute("SELECT pnl FROM daily_pnl WHERE d=?", (d,)).fetchone()
    pnl = sfloat(cur[0], 0.0) if cur else 0.0
    dd_pct = (-pnl / start_e * 100.0) if start_e > 0 and pnl < 0 else 0.0
    return dd_pct >= cfg.dd_day, dd_pct

# ---------- Основной цикл ----------
def main():
    con = sql_conn()
    tg("🚀 L1 бот (автокомпаунд, fault-tolerant) запущен.")
    if not sget(con, "L1_base_equity", ""):
        sset(con, "L1_base_equity", cfg.start_base)
    last_equity = total_equity()

    while True:
        try:
            # инициализация дневных метрик
            if daily_key() != sget(con, "last_day", ""):
                sset(con, "last_day", daily_key())
                sset(con, "day_start_equity", total_equity())

            day_start_equity = sfloat(sget(con, "day_start_equity", "0"), 0.0)
            if day_start_equity == 0.0:
                day_start_equity = total_equity()
                sset(con, "day_start_equity", day_start_equity)

            # лимит дневной просадки
            exceeded, dd = daily_drawdown_exceeded(con, day_start_equity)
            if exceeded:
                tg(f"⛔️ Дневной лимит просадки {cfg.dd_day}% достигнут ({dd:.2f}%). Пауза 1ч.")
                time.sleep(3600)
                continue

            eq = total_equity()
            update_daily_pnl(con, last_equity, eq); last_equity = eq
            free = free_equity()
            per_pair_alloc = max(0.0, eq * cfg.max_alloc)

            for sym in cfg.symbols:
                fr = funding_8h(sym)
                px = mark(sym)
                if px <= 0:
                    print(f"{now_s()} [{sym}] mark price unavailable, skip")
                    continue

                pos = positions(sym)
                hedged = (pos["spot"] > 1e-6) and (pos["perp"] < -1e-6) and (abs(pos["perp"]) >= pos["spot"] * 0.95)
                msg = f"[{sym}] FR(8h)={fr:.6f} px={px:.2f} hedged={hedged}"

                # вход
                can_enter = (not hedged) and (fr >= cfg.fr_thr) and (free >= max(per_pair_alloc, cfg.min_free)) and (not in_funding_window())
                if can_enter:
                    try:
                        base, o1 = order_spot_buy(sym, per_pair_alloc)
                        o2 = order_perp_sell(sym, base)
                        con.execute("INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                                    (now_s(), sym, "open_pair", base, per_pair_alloc, f"fr={fr}"))
                        con.commit()
                        tg(f"🟢 L1 OPEN {sym} • FR={fr:.5f} • alloc≈{per_pair_alloc:.2f} USDT")
                        time.sleep(2)
                        continue
                    except Exception as e:
                        print("open_pair error:", e)
                        tg(f"⚠️ Не удалось открыть связку {sym}: {e}")

                # выход по отрицательному funding
                if hedged and fr < -0.00005:
                    try:
                        order_close_pair(sym)
                        con.execute("INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                                    (now_s(), sym, "close_pair", 0, 0, f"fr={fr}"))
                        con.commit()
                        tg(f"🔴 L1 CLOSE {sym} • FR={fr:.5f}")
                        time.sleep(2)
                        continue
                    except Exception as e:
                        print("close_pair error:", e)
                        tg(f"⚠️ Не удалось закрыть связку {sym}: {e}")

                print(f"{now_s()} {msg} OK")

            time.sleep(cfg.poll)

        except ccxt.RateLimitExceeded:
            time.sleep(1.2)
        except ccxt.NetworkError as e:
            print("NetworkError:", e); time.sleep(2.0)
        except ccxt.ExchangeError as e:
            print("ExchangeError:", e); time.sleep(3.0)
        except Exception as e:
            print("Loop error:", e)
            tg(f"❗️L1 error: {e}")
            time.sleep(5.0)

if __name__ == "__main__":
    main()
