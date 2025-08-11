import os, time, math, sqlite3, datetime as dt
from typing import List, Dict, Any
import statistics

import ccxt
from pydantic import BaseModel, Field, field_validator
from telegram import Bot

DB_PATH = "/app/shared/ledger.db"

# ========== ENV-DEBUG ==========
TRACE_API = os.environ.get("TRACE_API", "false").lower() in {"1","true","yes","on"}
EXTRA_LOGS = os.environ.get("EXTRA_LOGS", "true").lower() in {"1","true","yes","on"}

# ---------- Утилиты ----------
def sfloat(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def now() -> dt.datetime:
    return dt.datetime.utcnow()

def now_s() -> str:
    return now().strftime("%Y-%m-%d %H:%M:%S")

def daily_key() -> str:
    return now().strftime("%Y-%m-%d")

def dlog(msg: str):
    if EXTRA_LOGS:
        print(msg)

# ---------- Конфиг ----------
class Cfg(BaseModel):
    # Bybit/API
    key: str = Field(..., alias="BYBIT_API_KEY")
    sec: str = Field(..., alias="BYBIT_API_SECRET")
    acct: str = Field(..., alias="BYBIT_ACCOUNT_TYPE")

    # Торговые параметры L1
    symbols: List[str] = Field(..., alias="L1_SYMBOLS")
    fr_thr: float = Field(..., alias="L1_FUNDING_THRESHOLD_8H")
    max_alloc: float = Field(..., alias="L1_MAX_ALLOC_PCT")
    lev: int = Field(..., alias="L1_PERP_LEVERAGE")
    min_free: float = Field(..., alias="L1_MIN_FREE_BALANCE_USDT")
    poll: int = Field(..., alias="L1_POLL_INTERVAL_SEC")
    dd_day: float = Field(..., alias="L1_MAX_DAILY_DD_PCT")

    # Автокомпаунд/переводы
    start_base: float = Field(..., alias="L1_START_BASE_USDT")
    pnl_thr_to_l2: float = Field(..., alias="L1_PNL_THRESHOLD_TO_L2")
    pnl_export_share: float = Field(..., alias="L1_PNL_EXPORT_SHARE")

    # Telegram
    tg_token: str = Field(..., alias="TG_BOT_TOKEN")
    tg_chat: str = Field(..., alias="TG_CHAT_ID")

    # Хук динамического порога funding + репорты по времени суток
    dyn_hook: bool = Field(False, alias="L1_DYN_HOOK_ENABLE")
    fr_lower: float = Field(0.00005, alias="L1_DYN_HOOK_FR_LOWER")
    fr_upper: float = Field(0.00020, alias="L1_DYN_HOOK_FR_UPPER")
    tz_offset_min: int = Field(0, alias="L1_TZ_OFFSET_MINUTES")  # смещение от UTC в минутах (например, МСК = +180)
    day_start_h: int = Field(9, alias="L1_DAY_START_HOUR")       # начало дневных часов (локальных)
    day_end_h: int = Field(21, alias="L1_DAY_END_HOUR")          # конец дневных часов (локальных, невключительно)
    report_top_n: int = Field(4, alias="L1_REPORT_TOP_N")

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

# ---------- Клиент биржи ----------
ex = ccxt.bybit({
    "apiKey": cfg.key,
    "secret": cfg.sec,
    "enableRateLimit": True,
    "options": {"defaultType": "unified"},
})
ex.load_markets()
ex.verbose = TRACE_API  # печатать сырые запросы/ответы ccxt при отладке


def to_perp_symbol(sym_spot: str) -> str:
    """
    Превращает 'BTC/USDT' -> 'BTC/USDT:USDT' (линейный перп).
    Если на бирже символ называется иначе, пытается найти swap-рынок по базе/квоте.
    """
    guess = f"{sym_spot}:USDT"
    if guess in ex.markets and ex.markets[guess].get("swap"):
        return guess

    base, quote = sym_spot.split("/")
    for m in ex.markets.values():
        if not m.get("swap"):
            continue
        if m.get("base") == base and m.get("quote") in (quote, "USDT"):
            return m["symbol"]

    dlog(f"[to_perp_symbol] не найден swap для {sym_spot}, fallback на спот")
    return sym_spot

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
    con.execute("INSERT OR REPLACE INTO state(k,v) VALUES(?,?)", (k, str(v)))
    con.commit()

# ---------- Безопасные обёртки к API ----------

def fetch_balance_safe() -> Dict[str, Dict[str, float]]:
    try:
        bal = ex.fetch_balance(params={"type": "unified"}) or {}
        if TRACE_API: dlog(f"[fetch_balance_safe] raw={bal}")
        total = {k: sfloat(v, 0.0) for k, v in (bal.get("total") or {}).items()}
        free = {k: sfloat(v, 0.0) for k, v in (bal.get("free") or {}).items()}
        used = {k: sfloat(v, 0.0) for k, v in (bal.get("used") or {}).items()}
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
    """Пытаемся взять last; если None — mid(bid,ask); если и это None — mid по книге"""
    try:
        t = ex.fetch_ticker(sym) or {}
        if TRACE_API: dlog(f"[mark] {sym} ticker={t}")
        last = sfloat(t.get("last"), 0.0)
        if last > 0:
            return last
        bid = sfloat(t.get("bid"), 0.0)
        ask = sfloat(t.get("ask"), 0.0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        ob = ex.fetch_order_book(sym)
        if TRACE_API: dlog(f"[mark] {sym} ob bests: bid={ob.get('bids',[[0]])[0][0] if ob.get('bids') else 0}, ask={ob.get('asks',[[0]])[0][0] if ob.get('asks') else 0}")
        best_bid = sfloat(ob.get("bids", [[0]])[0][0], 0.0) if ob.get("bids") else 0.0
        best_ask = sfloat(ob.get("asks", [[0]])[0][0], 0.0) if ob.get("asks") else 0.0
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        return 0.0
    except Exception as e:
        print("mark error:", e)
        return 0.0


def funding_8h(sym: str) -> float:
    """
    Возвращает ожидаемую ставку финансирования за 8ч для перп-контракта.
    Для Bybit используем ccxt.fetchFundingRate() по перп-символу (linear swap).
    """
    try:
        perp = to_perp_symbol(sym)
        fr = ex.fetchFundingRate(perp, params={"category": "linear"}) or {}
        if TRACE_API: dlog(f"[funding_8h] sym={sym} perp={perp} raw={fr}")
        rate = sfloat(fr.get("fundingRate"), 0.0)
        if rate == 0.0:
            info = fr.get("info") or {}
            rate = sfloat(info.get("fundingRate"), 0.0) or sfloat(info.get("funding_rate"), 0.0)
        return rate
    except Exception as e:
        print("funding_8h error:", e)
        return 0.0


def set_leverage(sym: str, lev: int):
    try:
        perp = to_perp_symbol(sym)
        try:
            ex.setLeverage(lev, perp, params={"marginMode": "cross"})
            dlog(f"[set_leverage] setLeverage {perp} -> {lev}x")
            return
        except Exception:
            pass
        mkt = ex.market(perp)
        ex.private_post_v5_position_set_leverage({
            "category": "linear",
            "symbol": mkt["id"],
            "buyLeverage": str(lev),
            "sellLeverage": str(lev),
        })
        dlog(f"[set_leverage] raw set_leverage {perp} -> {lev}x")
    except Exception as e:
        print("set_leverage error:", e)


def positions(sym: str) -> Dict[str, float]:
    base = sym.split("/")[0]
    bal = fetch_balance_safe()
    spot = sfloat(bal["total"].get(base), 0.0)
    perp_qty = 0.0
    try:
        perp = to_perp_symbol(sym)
        mkt = ex.market(perp)
        pos = ex.private_get_v5_position_list({"category": "linear", "symbol": mkt["id"]})
        lst = ((pos or {}).get("result") or {}).get("list") or []
        if TRACE_API: dlog(f"[positions] sym={sym} perp={perp} raw={pos}")
        for p in lst:
            side = (p.get("side") or "").lower()
            sz = sfloat(p.get("size"), 0.0)
            if side == "buy":
                perp_qty += sz
            elif side == "sell":
                perp_qty -= sz
    except Exception as e:
        print("positions error:", e)
    return {"spot": spot, "perp": perp_qty}


def order_spot_buy(sym: str, quote_usdt: float):
    px = mark(sym)
    if px <= 0:
        raise RuntimeError(f"mark price unavailable for {sym}")
    base = round((quote_usdt / px) * 0.998, 6)  # запас на комиссии
    o = ex.create_order(sym, type="market", side="buy", amount=base)
    if TRACE_API: dlog(f"[order_spot_buy] sym={sym} base={base} quote={quote_usdt} resp={o}")
    return base, o


def order_perp_sell(sym: str, base: float):
    set_leverage(sym, cfg.lev)
    perp = to_perp_symbol(sym)
    o = ex.create_order(perp, type="market", side="sell", amount=base, params={"reduceOnly": False})
    if TRACE_API: dlog(f"[order_perp_sell] perp={perp} base={base} resp={o}")
    return o


def order_close_pair(sym: str):
    pos = positions(sym)
    try:
        perp = to_perp_symbol(sym)
        if abs(pos["perp"]) > 1e-6:
            o1 = ex.create_order(perp, type="market",
                                 side=("buy" if pos["perp"] < 0 else "sell"),
                                 amount=abs(pos["perp"]), params={"reduceOnly": True})
            if TRACE_API: dlog(f"[order_close_pair] close perp={perp} qty={abs(pos['perp'])} resp={o1}")
        if pos["spot"] > 1e-6:
            o2 = ex.create_order(sym, type="market", side="sell", amount=pos["spot"])
            if TRACE_API: dlog(f"[order_close_pair] sell spot sym={sym} qty={pos['spot']} resp={o2}")
    except Exception as e:
        print("order_close_pair error:", e)

# ---------- Время суток и динамический порог ----------

def local_hour_24() -> int:
    """Текущий локальный час (0-23) с учётом cfg.tz_offset_min относительно UTC."""
    return int(((now() + dt.timedelta(minutes=cfg.tz_offset_min)).hour) % 24)

def is_daytime() -> bool:
    h = local_hour_24()
    return cfg.day_start_h <= h < cfg.day_end_h

def current_fr_threshold(fr_values: List[float]) -> float:
    """Динамический хук: если рынок "плоский" (медиана низкая) — снижаем порог, если горячий — поднимаем.
    В противном случае — базовый cfg.fr_thr."""
    if not cfg.dyn_hook or not fr_values:
        return cfg.fr_thr
    med = statistics.median(fr_values)
    # границы берём из конфигурации
    low, base, high = cfg.fr_lower, cfg.fr_thr, cfg.fr_upper
    if med <= (low + base) / 2:
        return low
    if med >= (base + high) / 2:
        return high
    return base


def in_funding_window() -> bool:
    # за 2–3 минуты до часа (00/08/16 UTC)
    t = now()
    return t.minute in (57, 58)

# ---------- Учёт/PNL ----------

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

    # для часового отчёта по funding
    last_report_tag = sget(con, "last_report_tag", "")  # формат YYYY-MM-DD_HH локальный

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
            update_daily_pnl(con, last_equity, eq)
            last_equity = eq
            free = free_equity()

            # ------- считываем FR по всем парам сразу и рассчитываем динамический порог -------
            fr_map: Dict[str, float] = {}
            px_map: Dict[str, float] = {}
            for sym in cfg.symbols:
                fr_map[sym] = funding_8h(sym)
                px_map[sym] = mark(sym)
            dyn_thr = current_fr_threshold(list(fr_map.values()))

            per_pair_alloc = max(0.0, eq * cfg.max_alloc)

            for sym in cfg.symbols:
                perp = to_perp_symbol(sym)
                fr = fr_map[sym]
                px = px_map[sym]
                if px <= 0:
                    dlog(f"{now_s()} [{sym}] perp={perp} mark price unavailable, skip")
                    continue

                pos = positions(sym)
                hedged = (pos["spot"] > 1e-6) and (pos["perp"] < -1e-6) and (abs(pos["perp"]) >= pos["spot"] * 0.95)
                msg = f"[{sym} | perp={perp}] FR(8h)={fr:.6f} (thr={dyn_thr:.6f}) px={px:.2f} hedged={hedged}"

                # вход
                can_enter = (not hedged) and (fr >= dyn_thr) and (free >= max(per_pair_alloc, cfg.min_free)) and (not in_funding_window())
                if can_enter:
                    try:
                        base, _ = order_spot_buy(sym, per_pair_alloc)
                        _ = order_perp_sell(sym, base)
                        con.execute(
                            "INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                            (now_s(), sym, "open_pair", base, per_pair_alloc, f"fr={fr}")
                        )
                        con.commit()
                        tg(f"🟢 L1 OPEN {sym} (perp {perp}) • FR={fr:.5f} thr={dyn_thr:.5f} • alloc≈{per_pair_alloc:.2f} USDT")
                        time.sleep(2)
                        continue
                    except Exception as e:
                        print("open_pair error:", e)
                        tg(f"⚠️ Не удалось открыть связку {sym} (perp {perp}): {e}")

                # выход по отрицательному funding
                if hedged and fr < -0.00005:
                    try:
                        order_close_pair(sym)
                        con.execute(
                            "INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                            (now_s(), sym, "close_pair", 0, 0, f"fr={fr}")
                        )
                        con.commit()
                        tg(f"🔴 L1 CLOSE {sym} (perp {perp}) • FR={fr:.5f}")
                        time.sleep(2)
                        continue
                    except Exception as e:
                        print("close_pair error:", e)
                        tg(f"⚠️ Не удалось закрыть связку {sym} (perp {perp}): {e}")

                print(f"{now_s()} {msg} OK")

            # ------- Часовой отчёт по funding только в дневные часы -------
            if is_daytime():
                tag = (now() + dt.timedelta(minutes=cfg.tz_offset_min)).strftime("%Y-%m-%d_%H")
                if tag != last_report_tag:
                    last_report_tag = tag
                    sset(con, "last_report_tag", last_report_tag)
                    # топ N по FR
                    top = sorted(fr_map.items(), key=lambda kv: kv[1], reverse=True)[:max(1, cfg.report_top_n)]
                    lines = [f"⏰ Дневной отчёт FR (локал.час {local_hour_24():02d}) • dyn_thr={dyn_thr:.5f}"]
                    for sym, frv in top:
                        lines.append(f"• {sym}: {frv:.5f}")
                    tg("
".join(lines))

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
