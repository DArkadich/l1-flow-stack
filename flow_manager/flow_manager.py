import os, time, sqlite3
import ccxt
from pydantic import BaseModel, Field
from telegram import Bot

DB_PATH = "/app/shared/ledger.db"

class Cfg(BaseModel):
    key: str = Field(..., alias="BYBIT_API_KEY")
    sec: str = Field(..., alias="BYBIT_API_SECRET")
    acct: str = Field(..., alias="BYBIT_ACCOUNT_TYPE")
    tg_token: str = Field(..., alias="TG_BOT_TOKEN")
    tg_chat: str = Field(..., alias="TG_CHAT_ID")
    start_base: float = Field(..., alias="L1_START_BASE_USDT")
    pnl_thr: float = Field(..., alias="L1_PNL_THRESHOLD_TO_L2")
    export_share: float = Field(..., alias="L1_PNL_EXPORT_SHARE")
    enable_transfer: bool = Field(..., alias="BYBIT_ENABLE_AUTO_TRANSFER")
    sub_l2: str = Field("", alias="BYBIT_L2_SUBACCOUNT_ID")
    asset: str = Field("USDT", alias="BYBIT_TRANSFER_ASSET")

cfg = Cfg(**os.environ)
bot = Bot(token=cfg.tg_token)
ex = ccxt.bybit({"apiKey": cfg.key, "secret": cfg.sec, "enableRateLimit": True, "options": {"defaultType": "unified"}})

def tg(msg: str):
    try: bot.send_message(chat_id=cfg.tg_chat, text=msg[:4000], disable_web_page_preview=True)
    except Exception as e: print("TG error:", e)

def sql_conn():
    con = sqlite3.connect(DB_PATH)
    return con

def total_equity():
    bal = ex.fetch_balance(params={"type":"unified"})
    return float(bal.get("total", {}).get("USDT", 0.0))

def available_usdt():
    bal = ex.fetch_balance(params={"type":"unified"})
    return float(bal.get("free", {}).get("USDT", 0.0))

def auto_transfer_to_sub(amount_usdt: float) -> str:
    # Варианты: перевод в субаккаунт по API. В ccxt => transfer()
    # fromAccount/toAccount значения специфичны для биржи; здесь используем "UNIFIED" -> "UNIFIED_SUBACCOUNT"
    # Требует корректной настройки субаккаунта и прав ключа в Bybit!
    try:
        params = {
            "transferId": ex.uuid(),
            "fromSubAccountId": None,
            "toSubAccountId": cfg.sub_l2,
        }
        res = ex.transfer(cfg.asset, amount_usdt, "UNIFIED", "UNIFIED", params)  # Bybit обрабатывает по subAccountId в params
        return f"OK:{res}"
    except Exception as e:
        return f"ERR:{e}"

def main():
    tg("🧭 Flow-manager запущен.")
    # базовая логика: раз в 5 минут проверяем прирост L1 vs стартовая база; если > порога — экспорт части прибыли в L2
    while True:
        try:
            con = sql_conn()
            eq = total_equity()
            start = cfg.start_base
            # прибыль L1 как (equity - start) — в простом варианте, т.к. L1 — единственный потребитель капитала в этом стеке
            pnl = max(0.0, eq - start)
            thr_val = start * cfg.pnl_thr
            if pnl >= thr_val:
                export_amt = pnl * cfg.export_share
                export_amt = max(0.0, min(export_amt, available_usdt()))
                if export_amt >= 10:  # не гоняем копейки
                    if cfg.enable_transfer and cfg.sub_l2:
                        res = auto_transfer_to_sub(export_amt)
                        status = "✅" if res.startswith("OK:") else "⚠️"
                        tg(f"{status} Авто-перевод {export_amt:.2f} {cfg.asset} из L1 → L2 (субаккаунт {cfg.sub_l2}). Результат: {res[:200]}")
                    else:
                        # Чёткая инструкция на ручной перевод (если авто отключён)
                        tg(
                            f"📤 Рекомендован перевод в L2: {export_amt:.2f} {cfg.asset}\n"
                            f"Причина: L1 прирос на {pnl:.2f} USDT (порог {thr_val:.2f}).\n"
                            f"Действие: Выполни внутренний трансфер на Bybit в субаккаунт L2 или на биржу/кошелёк L2.\n"
                            f"Подсказка: Bybit → Assets → Transfer → From: Unified(Main) → To: SubAccount(L2) → {cfg.asset} → {export_amt:.2f}"
                        )
                    # Обновляем «стартовую базу» под новую ступень, чтобы компаунд продолжался
                    new_start = start + export_amt
                    # сохраняем в state
                    con.execute("CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT)")
                    con.execute("INSERT OR REPLACE INTO state(k,v) VALUES(?,?)", ("L1_START_BASE_USDT", str(new_start)))
                    con.commit()
                    cfg.start_base = new_start
            con.close()
            time.sleep(300)
        except Exception as e:
            tg(f"❗️Flow-manager error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
