import os, time, math, sqlite3, datetime as dt
from typing import List, Dict
import ccxt
from pydantic import BaseModel, Field, validator
from telegram import Bot

DB_PATH = "/app/shared/ledger.db"

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

    @validator("symbols", pre=True)
    def parse_symbols(cls, v): return [s.strip() for s in str(v).split(",") if s.strip()]

cfg = Cfg(**os.environ)
bot = Bot(token=cfg.tg_token)

def tg(msg: str):
    try: bot.send_message(chat_id=cfg.tg_chat, text=msg[:4000], disable_web_page_preview=True)
    except Exception as e: print("TG error:", e)

ex = ccxt.bybit({"apiKey": cfg.key, "secret": cfg.sec, "enableRateLimit": True, "options": {"defaultType": "unified"}})

def sql_conn():
    os.makedirs("/app/shared", exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS state(
        k TEXT PRIMARY KEY, v TEXT);""")
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

def now(): return dt.datetime.utcnow()
def now_s(): return now().strftime("%Y-%m-%d %H:%M:%S")

def total_equity():
    bal = ex.fetch_balance(params={"type":"unified"})
    return float(bal.get("total", {}).get("USDT", 0.0))

def free_equity():
    bal = ex.fetch_balance(params={"type":"unified"})
    return float(bal.get("free", {}).get("USDT", 0.0))

def mark(sym):
    t = ex.fetch_ticker(sym); return float(t["last"])

def funding_8h(sym):
    mkt = ex.market(sym); fr = ex.public_get_v5_market_funding_rate({"symbol": mkt["id"]})
    return float(fr["result"]["list"][0]["fundingRate"])

def set_leverage(sym, lev):
    mkt = ex.market(sym)
    ex.private_post_v5_position_set_leverage({"category":"linear","symbol":mkt["id"],
        "buyLeverage": str(lev), "sellLeverage": str(lev)})

def positions(sym) -> Dict[str, float]:
    base = sym.split("/")[0]
    bal = ex.fetch_balance(params={"type":"unified"})
    spot = float(bal.get("total", {}).get(base, 0.0))
    mkt = ex.market(sym)
    pos = ex.private_get_v5_position_list({"category":"linear","symbol":mkt["id"]})
    qty = 0.0
    for p in pos.get("result",{}).get("list",[]):
        side = p.get("side"); sz = float(p.get("size",0.0))
        if side == "Buy": qty += sz
        elif side == "Sell": qty -= sz
    return {"spot": spot, "perp": qty}

def order_spot_buy(sym, quote_usdt):
    price = mark(sym); base = round((quote_usdt/price)*0.998, 6)
    o = ex.create_order(sym, type="market", side="buy", amount=base)
    return base, o

def order_perp_sell(sym, base):
    set_leverage(sym, cfg.lev)
    o = ex.create_order(sym, type="market", side="sell", amount=base, params={"reduceOnly": False})
    return o

def order_close_pair(sym):
    pos = positions(sym)
    if abs(pos["perp"]) > 1e-6:
        ex.create_order(sym, type="market", side=("buy" if pos["perp"]<0 else "sell"),
                        amount=abs(pos["perp"]), params={"reduceOnly": True})
    if pos["spot"] > 1e-6:
        ex.create_order(sym, type="market", side="sell", amount=pos["spot"])

def in_funding_window():
    t = now()
    return t.minute in (57,58)

def daily_key(): return now().strftime("%Y-%m-%d")

def update_daily_pnl(con, prev_equity, new_equity):
    d = daily_key()
    cur = con.execute("SELECT pnl FROM daily_pnl WHERE d=?", (d,)).fetchone()
    pnl = (new_equity - prev_equity) if prev_equity>0 else 0.0
    if cur: con.execute("UPDATE daily_pnl SET pnl=? WHERE d=?", (pnl,d))
    else:   con.execute("INSERT INTO daily_pnl(d,pnl) VALUES(?,?)", (d,pnl))
    con.commit()
    return pnl

def daily_drawdown_exceeded(con, start_e):
    d = daily_key()
    cur = con.execute("SELECT pnl FROM daily_pnl WHERE d=?", (d,)).fetchone()
    pnl = cur[0] if cur else 0.0
    dd_pct = (-pnl/start_e*100.0) if start_e>0 and pnl<0 else 0.0
    return dd_pct >= cfg.dd_day, dd_pct

def main():
    con = sql_conn()
    tg("🚀 L1 бот (автокомпаунд) запущен.")
    sset(con, "L1_base_equity", cfg.start_base)
    last_equity = total_equity()

    while True:
        try:
            if daily_key() != sget(con, "last_day", ""):
                sset(con, "last_day", daily_key())
                sset(con, "day_start_equity", total_equity())

            day_start_equity = float(sget(con, "day_start_equity", "0") or 0)
            if day_start_equity == 0:
                day_start_equity = total_equity(); sset(con, "day_start_equity", day_start_equity)

            # контроль дневной просадки
            exceeded, dd = daily_drawdown_exceeded(con, day_start_equity)
            if exceeded:
                tg(f"⛔️ Дневной лимит просадки {cfg.dd_day}% достигнут ({dd:.2f}%). Пауза на 1 час.")
                time.sleep(3600)
                continue

            eq = total_equity()
            update_daily_pnl(con, last_equity, eq); last_equity = eq
            free = free_equity()
            per_pair_alloc = eq * cfg.max_alloc

            for sym in cfg.symbols:
                fr = funding_8h(sym)
                price = mark(sym)
                pos = positions(sym)
                hedged = (pos["spot"] > 1e-6) and (pos["perp"] < -1e-6) and (abs(pos["perp"]) >= pos["spot"]*0.95)

                # вход
                if (not hedged) and fr >= cfg.fr_thr and free >= max(per_pair_alloc, cfg.min_free) and (not in_funding_window()):
                    base, o1 = order_spot_buy(sym, per_pair_alloc)
                    o2 = order_perp_sell(sym, base)
                    con.execute("INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                        (now_s(), sym, "open_pair", base, per_pair_alloc, f"fr={fr}"))
                    con.commit()
                    tg(f"🟢 L1 OPEN {sym} • FR(8h)={fr:.5f} • alloc≈{per_pair_alloc:.2f} USDT")
                    time.sleep(2)
                    continue

                # выход по обратному funding
                if hedged and fr < -0.00005:
                    order_close_pair(sym)
                    con.execute("INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                        (now_s(), sym, "close_pair", 0, 0, f"fr={fr}"))
                    con.commit()
                    tg(f"🔴 L1 CLOSE {sym} • FR(8h)={fr:.5f}")
                    time.sleep(2)
                    continue

            # автокомпаунд: просто растим equity; перевод в L2 делает менеджер по порогу
            time.sleep(cfg.poll)

        except ccxt.RateLimitExceeded:
            time.sleep(1.2)
        except ccxt.NetworkError as e:
            print("NetworkError:", e); time.sleep(2.0)
        except ccxt.ExchangeError as e:
            print("ExchangeError:", e); time.sleep(3.0)
        except Exception as e:
            print("Loop error:", e); tg(f"❗️L1 error: {e}"); time.sleep(5.0)

if __name__ == "__main__":
    main()
