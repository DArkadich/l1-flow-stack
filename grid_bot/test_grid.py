#!/usr/bin/env python3
"""
Тестовый скрипт для демонстрации Grid Trading бота
"""

import sys
import os
sys.path.append('/app')

from main import GridConfig, GridManager

def test_grid_creation():
    """Тестируем создание сеток"""
    print("🚀 Тестируем Grid Trading Bot")
    
    # Создаём конфигурацию
    config = GridConfig()
    print(f"✅ Конфигурация: {len(config.symbols)} пар")
    
    # Показываем пары
    for i, symbol in enumerate(config.symbols, 1):
        print(f"  {i}. {symbol}")
    
    # Тестируем создание сетки для DOGE/USDT
    symbol = "DOGE/USDT"
    current_price = 0.22  # Примерная цена
    
    print(f"\n📊 Создаём сетку для {symbol}")
    print(f"💰 Текущая цена: ${current_price}")
    print(f"⚙️ Параметры: {config.grid_levels} уровней, {config.grid_spread*100}% спред")
    
    # Создаём Grid Manager
    grid_manager = GridManager(None, config)
    
    # Создаём сетку
    grid_manager.create_grid(symbol, current_price)
    
    # Показываем созданную сетку
    if symbol in grid_manager.grids:
        grid = grid_manager.grids[symbol]
        print(f"\n📈 Сетка создана: {len(grid)} уровней")
        
        # Показываем уровни покупки
        buy_levels = [level for level in grid if level["side"] == "buy"]
        sell_levels = [level for level in grid if level["side"] == "sell"]
        
        print(f"\n🟢 Уровни покупки ({len(buy_levels)}):")
        for level in buy_levels:
            price = level["price"]
            amount = level["amount"]
            usdt_value = price * amount
            print(f"  ${price:.6f} → {amount:.4f} {symbol.split('/')[0]} (${usdt_value:.2f})")
        
        print(f"\n🔴 Уровни продажи ({len(sell_levels)}):")
        for level in sell_levels:
            price = level["price"]
            amount = level["amount"]
            usdt_value = price * amount
            print(f"  ${price:.6f} → {amount:.4f} {symbol.split('/')[0]} (${usdt_value:.2f})")
        
        # Расчёт ожидаемой доходности
        total_investment = len(buy_levels) * config.level_amount
        expected_profit_per_trade = config.level_amount * config.grid_spread
        
        print(f"\n💰 Инвестиции: ${total_investment:.2f}")
        print(f"📈 Прибыль за сделку: ${expected_profit_per_trade:.2f}")
        print(f"📊 ROI за сделку: {(config.grid_spread * 100):.1f}%")
        
        # Оценка дневной доходности
        trades_per_day = 8  # Примерно 8 сделок в день при высокой волатильности
        daily_profit = expected_profit_per_trade * trades_per_day
        daily_roi = (daily_profit / total_investment) * 100
        
        print(f"\n🎯 Ожидаемая доходность:")
        print(f"  📅 За день: ${daily_profit:.2f} ({daily_roi:.1f}%)")
        print(f"  📅 За месяц: ${daily_profit * 30:.2f} ({(daily_roi * 30):.1f}%)")
        
    else:
        print("❌ Ошибка создания сетки")

if __name__ == "__main__":
    test_grid_creation()
