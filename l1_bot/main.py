import os, time, math, sqlite3, datetime as dt
from typing import List, Dict, Any
import statistics
import time

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
    dd_min_eq: float = Field(200.0, alias="L1_DD_MIN_EQUITY_USDT")

    # Автокомпаунд/переводы
    start_base: float = Field(..., alias="L1_START_BASE_USDT")
    pnl_thr_to_l2: float = Field(..., alias="L1_PNL_THRESHOLD_TO_L2")
    pnl_export_share: float = Field(..., alias="L1_PNL_EXPORT_SHARE")

    # Telegram
    tg_token: str = Field(..., alias="TG_BOT_TOKEN")
    tg_chat: str = Field(..., alias="TG_CHAT_ID")
    # Отключать уведомления ночью (вне дневного окна)
    tg_night_mute: bool = Field(True, alias="TG_NIGHT_MUTE")

    # Динамический порог + дневные отчёты
    dyn_hook: bool = Field(False, alias="L1_DYN_HOOK_ENABLE")
    fr_lower: float = Field(0.001, alias="L1_DYN_HOOK_FR_LOWER")  # 0.1% - увеличен для лучшей маржинальности
    fr_upper: float = Field(0.003, alias="L1_DYN_HOOK_FR_UPPER")  # 0.3% - увеличен для лучшей маржинальности
    tz_offset_min: int = Field(0, alias="L1_TZ_OFFSET_MINUTES")  # смещение от UTC в минутах (МСК=180)
    day_start_h: int = Field(9, alias="L1_DAY_START_HOUR")       # [start, end) локальные часы
    day_end_h: int = Field(21, alias="L1_DAY_END_HOUR")
    report_top_n: int = Field(4, alias="L1_REPORT_TOP_N")
    report_min_fr: float = Field(0.0, alias="L1_REPORT_MIN_FR")  # фильтр в отчёте

    # Динамическое масштабирование аллокации под высокий FR
    alloc_scale_enable: bool = Field(True, alias="L1_ALLOC_SCALE_ENABLE")
    alloc_scale_k: float = Field(0.5, alias="L1_ALLOC_SCALE_K")
    alloc_scale_cap: float = Field(1.5, alias="L1_ALLOC_SCALE_CAP")

    # Исполнение и фильтры качества
    fr_extra_buffer: float = Field(0.00002, alias="L1_FR_EXTRA_BUFFER")
    max_spread_pct: float = Field(0.003, alias="L1_MAX_SPREAD_PCT")  # 0.3%

    # Выходы и гистерезис
    hysteresis_fr: float = Field(0.00002, alias="L1_HYST_FR")
    exit_fr_below_count: int = Field(3, alias="L1_EXIT_FR_BELOW_COUNT")
    max_hold_min: int = Field(30, alias="L1_MAX_HOLD_MIN")  # 30 минут - минимальное время удержания для снижения комиссий
    cooldown_min: int = Field(10, alias="L1_COOLDOWN_MIN")

    max_total_alloc: float = Field(0.6, alias="L1_MAX_TOTAL_ALLOC_PCT")
    max_pair_alloc_pct: float = Field(0.20, alias="L1_MAX_PAIR_ALLOC_PCT")  # 20% max per pair - увеличен для лучшей маржинальности
    # Принудительное закрытие через N часов удержания (0=выкл)
    force_close_after_h: int = Field(0, alias="L1_FORCE_CLOSE_AFTER_HOURS")
    # Maker-first (postOnly) с тайм-аутом fallback на market
    maker_fallback_ms: int = Field(3000, alias="L1_MAKER_FALLBACK_MS")

    # Snipe-режим вокруг funding payout
    snipe_enable: bool = Field(False, alias="L1_SNIPE_ENABLE")
    snipe_window_min: int = Field(12, alias="L1_SNIPE_WINDOW_MIN")
    snipe_min_fr: float = Field(0.00020, alias="L1_SNIPE_MIN_FR")
    snipe_close_after_min: int = Field(3, alias="L1_SNIPE_CLOSE_AFTER_MIN")


    # Доливка (scale-in) в уже открытые связки
    scale_in_enable: bool = Field(True, alias="L1_SCALEIN_ENABLE")
    scale_in_min_quote: float = Field(5.0, alias="L1_SCALEIN_MIN_QUOTE_USDT")
    scale_in_max_steps: int = Field(3, alias="L1_SCALEIN_MAX_STEPS_PER_DAY")
    scale_in_fr_buffer: float = Field(0.0, alias="L1_SCALEIN_FR_BUFFER")

    @field_validator("symbols", mode="before")
    @classmethod
    def parse_symbols(cls, v):
        return [s.strip() for s in str(v).split(",") if s.strip()]

cfg = Cfg(**os.environ)

# ---------- Telegram ----------
bot = Bot(token=cfg.tg_token)

def tg(msg: str, force: bool = False):
    """Отправка сообщения в TG. Ночные уведомления глушим, кроме критических.
    Критичными считаем сообщения, начинающиеся с ❗️ или ⛔️.
    """
    try:
        is_critical = str(msg).startswith("❗️") or str(msg).startswith("⛔️")
        if cfg.tg_night_mute and (not is_daytime()) and (not force) and (not is_critical):
            return
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
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=3000;")
    except Exception:
        pass
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


def is_marked_open(con, sym: str) -> bool:
    return sget(con, f"pair:{sym}:open", "0") == "1"


def mark_open(con, sym: str, opened: bool):
    sset(con, f"pair:{sym}:open", "1" if opened else "0")

# ---------- Безопасные обёртки к API ----------

def fetch_balance_safe() -> Dict[str, Dict[str, float]]:
    try:
        bal = ex.fetch_balance(params={"type": "unified"}) or {}
        if TRACE_API:
            dlog(f"[fetch_balance_safe] raw={bal}")
        total = {k: sfloat(v, 0.0) for k, v in (bal.get("total") or {}).items()}
        free = {k: sfloat(v, 0.0) for k, v in (bal.get("free") or {}).items()}
        used = {k: sfloat(v, 0.0) for k, v in (bal.get("used") or {}).items()}

        # Фоллбэк для Bybit UNIFIED: иногда ccxt возвращает None для free; берём из v5 wallet-balance
        usdt_total = sfloat(total.get("USDT"), 0.0)
        usdt_free = sfloat(free.get("USDT"), 0.0)
        if usdt_total > 0.0 and usdt_free == 0.0:
            try:
                acct = (cfg.acct or "UNIFIED").upper()
                wb = ex.private_get_v5_account_wallet_balance({"accountType": acct})
                coin_list = ((((wb or {}).get("result") or {}).get("list") or [{}])[0].get("coin") or [])
                for c in coin_list:
                    if (c.get("coin") or "").upper() == "USDT":
                        # availableBalance — эквивалент свободных средств для торговли
                        ab = sfloat(c.get("availableBalance"), 0.0)
                        if ab <= 0.0:
                            ab = sfloat(c.get("availableToWithdraw"), 0.0)
                        if ab <= 0.0:
                            # Оценка free через walletBalance - залоки/маржа/проценты
                            wbv = sfloat(c.get("walletBalance"), 0.0)
                            locked = sfloat(c.get("locked"), 0.0)
                            im_ord = sfloat(c.get("totalOrderIM"), 0.0)
                            im_pos = sfloat(c.get("totalPositionIM"), 0.0)
                            acci = sfloat(c.get("accruedInterest"), 0.0)
                            est = wbv - (locked + im_ord + im_pos + acci)
                            if est > 0.0:
                                ab = est
                        if ab > 0.0:
                            free["USDT"] = ab
                        break
            except Exception as e:
                if TRACE_API:
                    dlog(f"[fetch_balance_safe] v5 wallet-balance fallback error: {e}")
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


def spread_pct(sym: str) -> float:
    try:
        t = ex.fetch_ticker(sym) or {}
        bid = sfloat(t.get("bid"), 0.0)
        ask = sfloat(t.get("ask"), 0.0)
        if bid > 0 and ask > 0 and ask >= bid:
            mid = (bid + ask) / 2.0
            if mid > 0:
                return (ask - bid) / mid
        return 0.0
    except Exception as e:
        print("spread_pct error:", e)
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


def minutes_to_next_payout() -> int:
    t = now()
    windows = (0, 8, 16)
    nxt = None
    for i in range(24):
        c = (t + dt.timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
        if c.hour in windows and c > t:
            nxt = c
            break
    if nxt is None:
        nxt = (t + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(0, int((nxt - t).total_seconds() // 60))

def minutes_since_prev_payout() -> int:
    t = now()
    windows = (0, 8, 16)
    prev = None
    for i in range(24):
        c = (t - dt.timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
        if c.hour in windows and c < t:
            prev = c
            break
    if prev is None:
        prev = (t - dt.timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    return max(0, int((t - prev).total_seconds() // 60))

def in_snipe_open_window() -> bool:
    if not cfg.snipe_enable:
        return True
    return 0 < minutes_to_next_payout() <= max(1, cfg.snipe_window_min)

def in_snipe_close_window() -> bool:
    if not cfg.snipe_enable:
        return False
    return 0 <= minutes_since_prev_payout() <= max(1, cfg.snipe_close_after_min)

# ---------- Время суток, динамический порог, отчёты ----------

def min_quote_required(sym: str) -> float:
    """Минимально необходимая аллокация в USDT для корректного открытия связки
    (учёт минимального количества для спота и перпа). Возвращает 0.0 если данных нет.
    """
    try:
        px = mark(sym)
        if px <= 0:
            return 0.0
        # лимиты спота
        m_spot = ex.market(sym)
        lim_spot = (m_spot.get("limits") or {}).get("amount") or {}
        min_spot = sfloat(lim_spot.get("min"), 0.0)
        # лимиты перпа
        perp = to_perp_symbol(sym)
        m_perp = ex.market(perp)
        lim_perp = (m_perp.get("limits") or {}).get("amount") or {}
        min_perp = sfloat(lim_perp.get("min"), 0.0)
        # также учитываем стоимостные минимумы, если заданы
        cost_spot_min = sfloat(((m_spot.get("limits") or {}).get("cost") or {}).get("min"), 0.0)
        cost_perp_min = sfloat(((m_perp.get("limits") or {}).get("cost") or {}).get("min"), 0.0)
        base_min = max(min_spot, min_perp)
        if base_min <= 0.0:
            return 0.0
        # запас на комиссии 0.2%
        quote_req = (base_min * px) / 0.998
        if cost_spot_min > 0:
            quote_req = max(quote_req, cost_spot_min)
        if cost_perp_min > 0:
            quote_req = max(quote_req, cost_perp_min)
        return quote_req
    except Exception:
        return 0.0


def round_amount(sym: str, amount: float) -> float:
    try:
        m = ex.market(sym)
        prec = (m.get("precision") or {}).get("amount")
        if isinstance(prec, int) and prec >= 0:
            return float(f"{amount:.{prec}f}")
    except Exception:
        pass
    return round(amount, 6)

def local_datetime() -> dt.datetime:
    return now() + dt.timedelta(minutes=cfg.tz_offset_min)

def local_hour_24() -> int:
    return int(local_datetime().hour % 24)

def is_daytime() -> bool:
    h = local_hour_24()
    return cfg.day_start_h <= h < cfg.day_end_h

def should_send_9am_assets_report(tag_prev: str) -> bool:
    """Ежедневный отчёт активов в 09:00 локального времени, ровно один раз.
    tag_prev имеет формат YYYY-MM-DD_09.
    """
    dt_loc = local_datetime()
    tag_now = dt_loc.strftime("%Y-%m-%d_%H")
    if dt_loc.hour == 9 and tag_now != tag_prev:
        return True
    return False

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
    # отключаем контроль DD для малых депозитов, где mark-to-market шум непропорционален
    if start_e < max(1.0, cfg.dd_min_eq):
        return False, 0.0
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
    last_assets_report_tag = sget(con, "last_assets_report_tag", "")

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
            cap_per_pair = max(0.0, eq * max(0.0, min(cfg.max_pair_alloc_pct, 0.99)))
            for sym in cfg.symbols:
                perp = to_perp_symbol(sym)
                fr = fr_map[sym]
                px = px_map[sym]
                if px <= 0:
                    dlog(f"{now_s()} [{sym}] perp={perp} mark price unavailable, skip")
                    continue

                pos = positions(sym)
                hedged = (pos["spot"] > 1e-6) and (pos["perp"] < -1e-6) and (abs(pos["perp"]) >= pos["spot"] * 0.95)
                if is_marked_open(con, sym) and not hedged:
                    # пометка устарела — очищаем
                    mark_open(con, sym, False)
                msg = f"[{sym} | perp={perp}] FR(8h)={fr:.6f} (thr={dyn_thr:.6f}) px={px:.2f} hedged={hedged}"

                # динамическое масштабирование аллокации при высоком FR
                scaled_alloc = min(per_pair_alloc, cap_per_pair)
                if cfg.alloc_scale_enable and dyn_thr > 0:
                    excess = max(0.0, fr - dyn_thr)
                    scale = 1.0 + cfg.alloc_scale_k * (excess / max(dyn_thr, 1e-9))
                    scale = max(1.0, min(scale, cfg.alloc_scale_cap))
                    scaled_alloc = min(per_pair_alloc * scale, cap_per_pair)

                # учёт минимального размера ордера спота/перпа (в USDT)
                min_quote = min_quote_required(sym)
                # если минимально допустимая аллокация слишком велика относительно equity — пропускаем пару,
                # чтобы не ловить ошибки недостатка доступного баланса в Unified
                if min_quote > 0 and min_quote > eq * 0.8:
                    dlog(f"{now_s()} [{sym}] min_quote≈{min_quote:.2f} USDT > 80% equity≈{eq:.2f}, skip")
                    continue
                # учтём текущую стоимость уже купленного спота, чтобы не превысить cap на пару
                current_spot_quote = max(0.0, pos["spot"] * px)
                remaining_cap_for_pair = max(0.0, cap_per_pair - current_spot_quote)
                effective_alloc = max(scaled_alloc, min_quote)
                effective_alloc = min(effective_alloc, remaining_cap_for_pair) if remaining_cap_for_pair > 0 else 0.0

                # доп. фильтры: спред, FR-буфер, совокупная аллокация
                spr = spread_pct(sym)
                total_used_approx = max(0.0, eq - free)  # приблизительно: занято = equity - free
                total_after = total_used_approx + effective_alloc
                total_cap = eq * max(0.0, min(cfg.max_total_alloc, 0.99))

                # гистерезис удержания: снижение порога для проверки выхода (ниже)
                hold_thr = max(0.0, dyn_thr - cfg.hysteresis_fr)

                # вход
                can_enter = (
                    (not hedged)
                    and (fr >= (dyn_thr + cfg.fr_extra_buffer))
                    and (free >= max(effective_alloc, cfg.min_free))
                    and (not in_funding_quiet_period()) and (not cfg.snipe_enable or (in_snipe_open_window() and fr >= cfg.snipe_min_fr))
                    and (spr <= cfg.max_spread_pct)
                    and (total_after <= total_cap)
                )
                # cooldown
                cd_until = int(sfloat(sget(con, f"cooldown_until:{sym}", "0"), 0.0))
                now_ts = int(now().timestamp())
                not_in_cooldown = now_ts >= cd_until
                if EXTRA_LOGS:
                    dbg = {
                        "fr_ok": fr >= (dyn_thr + cfg.fr_extra_buffer),
                        "free_ok": free >= max(effective_alloc, cfg.min_free),
                        "not_quiet": not in_funding_quiet_period(), "snipe_ok": (not cfg.snipe_enable or (in_snipe_open_window() and fr >= cfg.snipe_min_fr)),
                        "spread_ok": spr <= cfg.max_spread_pct,
                        "cap_ok": total_after <= total_cap,
                        "not_hedged": not hedged,
                        "not_in_cooldown": not_in_cooldown,
                        "marked_open": is_marked_open(con, sym),
                        "eff_alloc": round(effective_alloc, 4),
                        "free": round(free, 4),
                        "spr": round(spr, 6),
                        "dyn_thr": round(dyn_thr, 6),
                        "fr": round(fr, 6),
                        "total_after": round(total_after, 4),
                        "total_cap": round(total_cap, 4),
                    }
                    print(f"{now_s()} [ENTER_CHECK] {sym} {dbg}")

                if can_enter and not is_marked_open(con, sym) and not_in_cooldown and effective_alloc >= min_quote:
                    try:
                        mark_open(con, sym, True)
                        # 1) сначала открываем перп-шорт, чтобы не съесть USDT под маржу покупкой спота
                        px_enter = px
                        base = round_amount(sym, (effective_alloc / px_enter) * 0.998)
                        try:
                            _ = order_perp_sell(sym, base)
                        except Exception as e:
                            mark_open(con, sym, False)
                            raise e

                        # 2) затем покупаем спот тем же количеством базовой валюты
                        try:
                            _ = ex.create_order(sym, type="market", side="buy", amount=base)
                        except Exception as e:
                            # откатываем перп при неуспехе спота
                            try:
                                perp = to_perp_symbol(sym)
                                _ = ex.create_order(perp, type="market", side="buy", amount=base, params={"reduceOnly": True})
                            except Exception as e2:
                                print("compensation close perp failed:", e2)
                            mark_open(con, sym, False)
                            raise e
                        # отметка времени открытия
                        sset(con, f"open_ts:{sym}", str(now_ts))
                        con.execute(
                            "INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                            (now_s(), sym, "open_pair", base, effective_alloc, f"fr={fr} min_quote={min_quote:.4f}")
                        )
                        con.commit()
                        tg(f"🟢 L1 OPEN {sym} (perp {perp}) • FR={fr:.5f} thr={dyn_thr:.5f} • alloc≈{effective_alloc:.2f} USDT")
                        time.sleep(2)
                        # сбрасываем пометку, чтобы не мешать повторным входам в будущем
                        mark_open(con, sym, False)
                        continue
                    except Exception as e:
                        print("open_pair error:", e)
                        tg(f"⚠️ Не удалось открыть связку {sym} (perp {perp}): {e}")

                # выход по отрицательному funding
                below_key = f"below_thr_count:{sym}"
                if hedged:
                    if fr < hold_thr:
                        cnt = int(sfloat(sget(con, below_key, "0"), 0.0)) + 1
                        sset(con, below_key, str(cnt))
                    else:
                        if sget(con, below_key, "0") != "0":
                            sset(con, below_key, "0")

                exit_due_to_negative = hedged and (fr < -0.00005)
                exit_due_to_below = hedged and (int(sfloat(sget(con, below_key, "0"), 0.0)) >= cfg.exit_fr_below_count)

                # тайм-аут удержания
                exit_due_to_time = False
                if hedged and cfg.max_hold_min > 0:
                    ots = int(sfloat(sget(con, f"open_ts:{sym}", "0"), 0.0))
                    if ots > 0:
                        held_min = max(0, int((now_ts - ots) // 60))
                        exit_due_to_time = (held_min >= cfg.max_hold_min) and (fr < dyn_thr) or (cfg.snipe_enable and in_snipe_close_window())
                        # принудительное закрытие после N часов независимо от FR
                        if cfg.force_close_after_h > 0 and held_min >= max(1, cfg.force_close_after_h) * 60:
                            exit_due_to_time = True

                if exit_due_to_negative or exit_due_to_below or exit_due_to_time:
                    try:
                        order_close_pair(sym)
                        con.execute(
                            "INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                            (now_s(), sym, "close_pair", 0, 0, f"fr={fr}")
                        )
                        con.commit()
                        tg(f"🔴 L1 CLOSE {sym} (perp {perp}) • FR={fr:.5f}")
                        # сброс счётчика и установка cooldown
                        sset(con, below_key, "0")
                        cd_until = now_ts + max(0, cfg.cooldown_min) * 60
                        sset(con, f"cooldown_until:{sym}", str(cd_until))
                        time.sleep(2)
                        continue
                    except Exception as e:
                        print("close_pair error:", e)
                        tg(f"⚠️ Не удалось закрыть связку {sym} (perp {perp}): {e}")

                print(f"{now_s()} {msg} OK")

                # --------- ДОЛИВКА (scale-in) при высоком FR ---------
                if cfg.scale_in_enable and hedged:
                    # проверяем дневной лимит шагов
                    key_steps = f"scalein_steps:{daily_key()}:{sym}"
                    steps = int(sfloat(sget(con, key_steps, "0"), 0.0))
                    if steps < max(0, cfg.scale_in_max_steps):
                        # условия доливки: FR выше порога + буфер, спред ок, есть свободные средства и не в тихом окне
                        can_scale = (
                            fr >= (dyn_thr + max(0.0, cfg.scale_in_fr_buffer))
                            and spr <= cfg.max_spread_pct
                            and (not in_funding_quiet_period()) and (not cfg.snipe_enable or (in_snipe_open_window() and fr >= cfg.snipe_min_fr))
                        )
                        if can_scale:
                            # размер доливки: минимум из scale_in_min_quote и доступного free, но не превышаем cap
                            # не превышаем лимит на пару с учётом текущего спота
                            current_spot_quote = max(0.0, positions(sym)["spot"] * px)
                            remaining_cap_for_pair = max(0.0, cap_per_pair - current_spot_quote)
                            alloc_si = min(cfg.scale_in_min_quote, free, remaining_cap_for_pair)
                            total_after_si = total_used_approx + alloc_si
                            if alloc_si >= cfg.scale_in_min_quote and total_after_si <= total_cap:
                                try:
                                    # доливка: купить спот на alloc_si и долить перп шорт на то же количество базы
                                    base_add, _ = order_spot_buy(sym, alloc_si)
                                    try:
                                        _ = order_perp_sell(sym, base_add)
                                    except Exception as e:
                                        # если перп не смогли — откатываем спот
                                        try:
                                            _ = ex.create_order(sym, type="market", side="sell", amount=base_add)
                                        except Exception as e2:
                                            print("scale-in compensation sell spot failed:", e2)
                                        raise e
                                    steps += 1
                                    sset(con, key_steps, str(steps))
                                    con.execute(
                                        "INSERT INTO trades(ts,sym,action,base,quote,info) VALUES(?,?,?,?,?,?)",
                                        (now_s(), sym, "scale_in", base_add, alloc_si, f"fr={fr}")
                                    )
                                    con.commit()
                                    tg(f"🟦 L1 SCALE-IN {sym} • FR={fr:.5f} • +≈{alloc_si:.2f} USDT")
                                    time.sleep(1)
                                except Exception as e:
                                    print("scale_in error:", e)
                                    tg(f"⚠️ Не удалось долить {sym}: {e}")

            # ------- ЕЖЕДНЕВНЫЙ ОТЧЁТ АКТИВОВ В 09:00 ЛОКАЛЬНО -------
            if should_send_9am_assets_report(last_assets_report_tag):
                last_assets_report_tag = local_datetime().strftime("%Y-%m-%d_%H")
                sset(con, "last_assets_report_tag", last_assets_report_tag)
                try:
                    total = total_equity()
                    free_b = free_equity()
                    tg(f"📊 Ежедневный отчёт (09:00): общий баланс≈{total:.2f} USDT, свободно≈{free_b:.2f} USDT", force=True)
                except Exception as e:
                    print("assets_report error:", e)

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
