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

    # Динамический порог + дневные отчёты
    dyn_hook: bool = Field(False, alias="L1_DYN_HOOK_ENABLE")
    fr_lower: float = Field(0.00005, alias="L1_DYN_HOOK_FR_LOWER")
    fr_upper: float = Field(0.00020, alias="L1_DYN_HOOK_FR_UPPER")
    tz_offset_min: int = Field(0, alias="L1_TZ_OFFSET_MINUTES")  # смещение от UTC в минутах (МСК=180)
    day_start_h: int = Field(9, alias="L1_DAY_START_HOUR")       # [start, end) локальные часы
    day_end_h: int = Field(21, alias="L1_DAY_END_HOUR")
    report_top_n: int = Field(4, alias="L1_REPORT_TOP_N")
    report_min_fr: float = Field(0.0, alias="L1_REPORT_MIN_FR")  # фильтр в отчёте

    # Динамическое масштабирование аллокации под высокий FR
    alloc_scale_enable: bool = Field(True, alias="L1_ALLOC_SCALE_ENABLE")
    alloc_scale_k: float = Field(0.5, alias="L1_ALLOC_SCALE_K")
    alloc_scale_cap: float = Field(1.5, alias="L1_ALLOC_SCALE_CAP")

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
ex.verbose = TRACE_API


def to_perp_symbol(sym_spot: str) -> str:
    """ 'BTC/USDT' -> 'BTC/USDT:USDT' (linear swap). Если не найдено — пытаемся поискать по базе/квоте. """
    guess = f"{sym_spot}:USDT"
    if guess in ex.markets and ex.markets[guess].get("swap"):
        return guess
    base, quote = sym_spot.split("/")
    for m in ex.markets.values():
        if m.get("swap") and m.get("base") == base and m.get("quote") in (quote, "USDT"):
            return m["symbol"]
    dlog(f"[to_perp_symbol] swap не найден для {sym_spot}, fallback на спот")
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
        if TRACE_API:
            dlog(f"[fetch_balance_safe] raw={bal}")
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
    """ Берём last; если None — mid(bid,ask); иначе mid по книге. """
    try:
        t = ex.fetch_ticker(sym) or {}
        if TRACE_API:
            dlog(f"[mark] {sym} ticker={t}")
        last = sfloat(t.get("last"), 0.0)
        if last > 0:
            return last
        bid = sfloat(t.get("bid"), 0.0)
        ask = sfloat(t.get("ask"), 0.0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        ob = ex.fetch_order_book(sym)
        best_bid = sfloat(ob.get("bids", [[0]])[0][0], 0.0) if ob.get("bids") else 0.0
        best_ask = sfloat(ob.get("asks", [[0]])[0][0], 0.0) if ob.get("asks") else 0.0
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        return 0.0
    except Exception as e:
        print("mark error:", e)
        return 0.0


def funding_8h(sym: str) -> float:
    """ Ожидаемая ставка финансирования за 8ч (Bybit linear swap через ccxt.fetchFundingRate). """
    try:
        perp = to_perp_symbol(sym)
        fr = ex.fetchFundingRate(perp, params={"category": "linear"}) or {}
        if TRACE_API:
            dlog(f"[funding_8h] sym={sym} perp={perp} raw={fr}")
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
        if TRACE_API:
            dlog(f"[positions] sym={sym} perp={perp} raw={pos}")
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
    if TRACE_API:
        dlog(f"[order_spot_buy] sym={sym} base={base} quote={quote_usdt} resp={o}")
    return base, o


def order_perp_sell(sym: str, base: float):
    set_leverage(sym, cfg.lev)
    perp = to_perp_symbol(sym)
    o = ex.create_order(perp, type="market", side="sell", amount=base, params={"reduceOnly": False})
    if TRACE_API:
        dlog(f"[order_perp_sell] perp={perp} base={base} resp={o}")
    return o


def order_close_pair(sym: str):
    pos = positions(sym)
    try:
        perp = to_perp_symbol(sym)
        if abs(pos["perp"]) > 1e-6:
            o1 = ex.create_order(perp, type="market",
                                 side=("buy" if pos["perp"] < 0 else "sell"),
                                 amount=abs(pos["perp"]), params={"reduceOnly": True})
            if TRACE_API:
                dlog(f"[order_close_pair] close perp={perp} qty={abs(pos['perp'])} resp={o1}")
        if pos["spot"] > 1e-6:
            o2 = ex.create_order(sym, type="market", side="sell", amount=pos["spot"])
            if TRACE_API:
                dlog(f"[order_close_pair] sell spot sym={sym} qty={pos['spot']} resp={o2}")
    except Exception as e:
        print("order_close_pair error:", e)

# ---------- Время суток, динамический порог, отчёты ----------

def local_datetime() -> dt.datetime:
    return now() + dt.timedelta(minutes=cfg.tz_offset_min)

def local_hour_24() -> int:
    return int(local_datetime().hour % 24)

def is_daytime() -> bool:
    h = local_hour_24()
    return cfg.day_start_h <= h < cfg.day_end_h

def minutes_to_next_funding_window() -> int:
    """ Funding выплата на 00:00, 08:00, 16:00 UTC. Считаем минуты до ближайшего окна. """
    t = now()
    windows = [0, 8, 16]
    # следующий целый час UTC среди окон
    next_hour = None
    for i in range(24):
        cand = (t + dt.timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
        if cand.hour in windows and cand > t:
            next_hour = cand
            break
    if next_hour is None:
        next_hour = (t + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(0, int((next_hour - t).total_seconds() // 60))


def current_fr_threshold(fr_values: List[float]) -> float:
    """ Динамический порог: используем перцентили p25/p75 для оценки "плоскости" рынка. """
    if not cfg.dyn_hook or not fr_values:
        return cfg.fr_thr
    try:
        qs = statistics.quantiles(fr_values, n=4, method='inclusive')  # [Q1, Q2, Q3]
        q1, q2, q3 = qs[0], qs[1], qs[2]
        med = q2
        low, base, high = cfg.fr_lower, cfg.fr_thr, cfg.fr_upper
        if med <= q1:
            return low
        if med >= q3:
            return high
        return base
    except Exception:
        # fallback на медиану
        med = statistics.median(fr_values)
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


def minutes_since_prev_funding_window() -> int:
    """Минут с момента предыдущего окна выплаты funding (00:00, 08:00, 16:00 UTC)."""
    t = now()
    windows = [0, 8, 16]
    prev_point = None
    for i in range(24):
        cand = (t - dt.timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
        if cand.hour in windows and cand < t:
            prev_point = cand
            break
    if prev_point is None:
        prev_point = (t - dt.timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    return max(0, int((t - prev_point).total_seconds() // 60))


def in_funding_quiet_period() -> bool:
    """Тихое окно вокруг payout: не входим за 5 минут до него и 2 минуты после."""
    return minutes_to_next_funding_window() <= 5 or minutes_since_prev_funding_window() <= 2

# ---------- Учёт/PNL ----------

def update_daily_pnl(con, day_start_equity: float, current_equity: float):
    """Сохраняет накопленный дневной PnL: equity_today - day_start_equity."""
    d = daily_key()
    pnl_today = (current_equity - day_start_equity) if day_start_equity > 0 else 0.0
    cur = con.execute("SELECT pnl FROM daily_pnl WHERE d=?", (d,)).fetchone()
    if cur:
        con.execute("UPDATE daily_pnl SET pnl=? WHERE d=?", (pnl_today, d))
    else:
        con.execute("INSERT INTO daily_pnl(d,pnl) VALUES(?,?)", (d, pnl_today))
    con.commit()
    return pnl_today


def daily_drawdown_exceeded(con, start_e: float):
    d = daily_key()
    cur = con.execute("SELECT pnl FROM daily_pnl WHERE d=?", (d,)).fetchone()
    pnl_today = sfloat(cur[0], 0.0) if cur else 0.0
    dd_pct = (-(pnl_today) / start_e * 100.0) if start_e > 0 and pnl_today < 0 else 0.0
    return dd_pct >= cfg.dd_day, dd_pct

# ---------- Основной цикл ----------

def main():
    con = sql_conn()
    tg("🚀 L1 бот (автокомпаунд, дневные отчёты, dyn-threshold) запущен.")
    # Синхронизация стартовой базы с SQLite
    saved_base = sget(con, "L1_START_BASE_USDT", "")
    if saved_base:
        cfg.start_base = sfloat(saved_base, cfg.start_base)
    else:
        sset(con, "L1_START_BASE_USDT", cfg.start_base)
    last_equity = total_equity()

    last_report_tag = sget(con, "last_report_tag", "")  # YYYY-MM-DD_HH (локально)

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
            update_daily_pnl(con, day_start_equity, eq)
            last_equity = eq
            free = free_equity()

            # ------- FR по всем парам + dyn threshold -------
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

                # динамическое масштабирование аллокации при высоком FR
                scaled_alloc = per_pair_alloc
                if cfg.alloc_scale_enable and dyn_thr > 0:
                    excess = max(0.0, fr - dyn_thr)
                    scale = 1.0 + cfg.alloc_scale_k * (excess / max(dyn_thr, 1e-9))
                    scale = max(1.0, min(scale, cfg.alloc_scale_cap))
                    scaled_alloc = per_pair_alloc * scale

                # вход
                can_enter = (
                    (not hedged)
                    and (fr >= dyn_thr)
                    and (free >= max(scaled_alloc, cfg.min_free))
                    and (not in_funding_quiet_period())
                )
                if can_enter:
                    try:
                        base, _ = order_spot_buy(sym, scaled_alloc)
                        try:
                            _ = order_perp_sell(sym, base)
                        except Exception as e:
                            # компенсируем спот, если перп не открылся
                            try:
                                _ = ex.create_order(sym, type="market", side="sell", amount=base)
                            except Exception as e2:
                                print("compensation sell spot failed:", e2)
                            raise e
                        con.execute(
                            "INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                            (now_s(), sym, "open_pair", base, scaled_alloc, f"fr={fr}")
                        )
                        con.commit()
                        tg(f"🟢 L1 OPEN {sym} (perp {perp}) • FR={fr:.5f} thr={dyn_thr:.5f} • alloc≈{scaled_alloc:.2f} USDT")
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
                tag = local_datetime().strftime("%Y-%m-%d_%H")
                if tag != last_report_tag:
                    last_report_tag = tag
                    sset(con, "last_report_tag", last_report_tag)
                    mins = minutes_to_next_funding_window()
                    # фильтр по минимальному FR и сортировка по убыванию
                    pairs = [(sym, fr) for sym, fr in fr_map.items() if fr >= cfg.report_min_fr]
                    pairs.sort(key=lambda kv: kv[1], reverse=True)
                    top = pairs[:max(1, cfg.report_top_n)]
                    lines = [
                        f"⏰ Дневной отчёт FR (локал.час {local_hour_24():02d}) • dyn_thr={dyn_thr:.5f} • мин до payout≈{mins}"
                    ]
                    for sym, frv in top:
                        lines.append(f"• {sym}: {frv:.5f}")
                    if len(top) == 0:
                        lines.append(f"• Нет пар ≥ {cfg.report_min_fr:.5f}")
                    tg("\n".join(lines))

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
