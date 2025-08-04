# modules/logger.py

import logging
import os
from datetime import datetime


class TradingLogger:
    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger('trading_bot')
        self.logger.setLevel(log_level)

        if not os.path.exists('logs'):
            os.makedirs('logs')

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = logging.FileHandler('logs/trading.log', encoding='utf-8')
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Убираем цветное форматирование для консоли
        if hasattr(console_handler.stream, 'reconfigure'):
            console_handler.stream.reconfigure(encoding='utf-8', errors='replace')

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def macd_crossover(self, direction, macd, signal, price):
        direction_ru = "ВВЕРХ" if direction == "BULLISH" else "ВНИЗ"
        self.info(f"ПЕРЕСЕЧЕНИЕ MACD - {direction_ru} | MACD: {macd:.6f} | Signal: {signal:.6f} | Цена: {price}")

    def position_opened(self, side, size, price):
        side_ru = "LONG" if side == "Buy" else "SHORT"
        self.info(f"ПОЗИЦИЯ ОТКРЫТА - {side_ru} | Размер: {size} USDT | Цена: {price}")

    def position_closed(self, side, price, pnl=None):
        side_ru = "LONG" if side == "Buy" else "SHORT"
        pnl_text = f" | PnL: {pnl}" if pnl else ""
        self.info(f"ПОЗИЦИЯ ЗАКРЫТА - {side_ru} | Цена: {price}{pnl_text}")

    def crossover_check(self, maintained, action):
        maintained_ru = "Да" if maintained else "Нет"
        action_ru = {
            "WAITING_FOR_OPPOSITE": "ОЖИДАНИЕ ОБРАТНОГО ПЕРЕСЕЧЕНИЯ",
            "REVERSING_POSITION": "РАЗВОРОТ ПОЗИЦИИ"
        }.get(action, action)
        self.info(f"ПРОВЕРКА ПЕРЕСЕЧЕНИЯ - Сохранилось: {maintained_ru} | Действие: {action_ru}")

    def api_error(self, exchange, error):
        self.error(f"ОШИБКА API - {exchange}: {error}")

    def insufficient_funds(self, required, available):
        self.warning(f"НЕДОСТАТОЧНО СРЕДСТВ - Требуется: {required} | Доступно: {available}")

    def reconnection(self, service):
        self.warning(f"ПЕРЕПОДКЛЮЧЕНИЕ - {service}")

    def startup(self):
        self.info("=" * 50)
        self.info("ТОРГОВЫЙ БОТ ЗАПУЩЕН")
        self.info("=" * 50)