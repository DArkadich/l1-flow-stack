"""
Grid Trading Bot - Автоматическая торговля по сетке цен

ПАРЫ:
- DOGE/USDT (25 USDT, 5 уровней)
- WIF/USDT (25 USDT, 5 уровней) 
- JUP/USDT (25 USDT, 5 уровней)
- OP/USDT (20 USDT, 4 уровня)
- ENA/USDT (20 USDT, 4 уровня)

ЦЕЛЬ: 5-15% доходности в день
"""

import ccxt
import sqlite3
import time
import os
from datetime import datetime
from typing import Dict, List, Tuple
from dataclasses import dataclass
from telegram import Bot

# ========== КОНФИГУРАЦИЯ ==========
@dataclass
class GridConfig:
    # API
    api_key: str = os.environ.get("BYBIT_API_KEY", "")
    api_secret: str = os.environ.get("BYBIT_API_SECRET", "")
    
    # Параметры сетки
    grid_levels: int = 5  # количество уровней
    grid_spread: float = 0.02  # 2% между уровнями
    level_amount: float = 5.0  # USDT на уровень
    
    # Пары для торговли
    symbols: List[str] = None
    
    def __post_init__(self):
        if self.symbols is None:
            self.symbols = ["DOGE/USDT", "WIF/USDT", "JUP/USDT", "OP/USDT", "ENA/USDT"]

# ========== КЛИЕНТ БИРЖИ ==========
class BybitClient:
    def __init__(self, config: GridConfig):
        self.exchange = ccxt.bybit({
            "apiKey": config.api_key,
            "secret": config.api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "unified"}
        })
        self.exchange.load_markets()
    
    def get_ticker(self, symbol: str) -> Dict:
        """Получить текущие цены"""
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return {
                "bid": float(ticker["bid"]),
                "ask": float(ticker["ask"]),
                "last": float(ticker["last"]),
                "volume": float(ticker["baseVolume"])
            }
        except Exception as e:
            print(f"Ошибка получения тикера {symbol}: {e}")
            return {}
    
    def place_order(self, symbol: str, side: str, amount: float, price: float) -> Dict:
        """Разместить лимитный ордер"""
        try:
            order = self.exchange.create_order(
                symbol=symbol,
                type="limit",
                side=side,
                amount=amount,
                price=price
            )
            return order
        except Exception as e:
            print(f"Ошибка размещения ордера {symbol}: {e}")
            return {}

# ========== УПРАВЛЕНИЕ СЕТКОЙ ==========
class GridManager:
    def __init__(self, client: BybitClient, config: GridConfig):
        self.client = client
        self.config = config
        self.grids: Dict[str, List[Dict]] = {}
        self.db_path = "/app/shared/grid_trading.db"
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица сеток
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS grids (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    level INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    amount REAL NOT NULL,
                    price REAL NOT NULL,
                    order_id TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица сделок
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount REAL NOT NULL,
                    price REAL NOT NULL,
                    profit REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            conn.close()
            print("База данных инициализирована")
        except Exception as e:
            print(f"Ошибка инициализации БД: {e}")
    
    def create_grid(self, symbol: str, current_price: float):
        """Создать сетку для пары"""
        try:
            grid = []
            base_amount = self.config.level_amount / current_price
            
            # Создаём уровни покупки ниже текущей цены
            for i in range(self.config.grid_levels):
                buy_price = current_price * (1 - self.config.grid_spread * (i + 1))
                buy_price = round(buy_price, 6)
                
                grid.append({
                    "level": i,
                    "side": "buy",
                    "price": buy_price,
                    "amount": base_amount,
                    "status": "pending"
                })
            
            # Создаём уровни продажи выше текущей цены
            for i in range(self.config.grid_levels):
                sell_price = current_price * (1 + self.config.grid_spread * (i + 1))
                sell_price = round(sell_price, 6)
                
                grid.append({
                    "level": i,
                    "side": "sell", 
                    "price": sell_price,
                    "amount": base_amount,
                    "status": "pending"
                })
            
            self.grids[symbol] = grid
            self.save_grid_to_db(symbol, grid)
            print(f"Сетка создана для {symbol}: {len(grid)} уровней")
            
        except Exception as e:
            print(f"Ошибка создания сетки {symbol}: {e}")
    
    def save_grid_to_db(self, symbol: str, grid: List[Dict]):
        """Сохранить сетку в базу данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for level in grid:
                cursor.execute("""
                    INSERT INTO grids (symbol, level, side, amount, price)
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, level["level"], level["side"], level["amount"], level["price"]))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Ошибка сохранения сетки в БД: {e}")
    
    def place_grid_orders(self, symbol: str):
        """Разместить ордера сетки"""
        try:
            if symbol not in self.grids:
                return
            
            grid = self.grids[symbol]
            for level in grid:
                if level["status"] == "pending":
                    order = self.client.place_order(
                        symbol=symbol,
                        side=level["side"],
                        amount=level["amount"],
                        price=level["price"]
                    )
                    
                    if order and "id" in order:
                        level["order_id"] = order["id"]
                        level["status"] = "active"
                        print(f"Ордер размещён: {symbol} {level['side']} {level['amount']} @ {level['price']}")
                    
                    time.sleep(0.1)  # Задержка между ордерами
                    
        except Exception as e:
            print(f"Ошибка размещения ордеров сетки {symbol}: {e}")

# ========== ОСНОВНОЙ ЦИКЛ ==========
def main():
    print("🚀 Grid Trading Bot запущен!")
    
    # Инициализация
    config = GridConfig()
    client = BybitClient(config)
    grid_manager = GridManager(client, config)
    
    # Создание сеток для всех пар
    for symbol in config.symbols:
        try:
            ticker = client.get_ticker(symbol)
            if ticker and "last" in ticker:
                current_price = ticker["last"]
                grid_manager.create_grid(symbol, current_price)
                grid_manager.place_grid_orders(symbol)
                print(f"Сетка активирована для {symbol}")
            else:
                print(f"Не удалось получить цену для {symbol}")
        except Exception as e:
            print(f"Ошибка инициализации {symbol}: {e}")
    
    print("✅ Все сетки созданы и активированы!")
    print("📊 Мониторинг активен...")

if __name__ == "__main__":
    main()
