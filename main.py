# main.py

import os
import threading
import time
import signal
import sys
from dotenv import load_dotenv

from modules.logger import TradingLogger
from modules.data_manager import DataManager
from modules.trading_engine import TradingEngine
from modules.strategy_engine import StrategyEngine


class TradingBot:
    def __init__(self):
        load_dotenv()

        self.logger = TradingLogger()
        self.logger.startup()

        self.config = self.load_config()

        self.data_manager = DataManager(
            symbol=self.config['SYMBOL'],
            timeframe=self.config['TIMEFRAME'],
            logger=self.logger,
            config=self.config  # Передаем конфиг в data_manager
        )

        self.trading_engine = TradingEngine(
            api_key=self.config['BYBIT_API_KEY'],
            secret=self.config['BYBIT_SECRET'],
            symbol=self.config['SYMBOL'],
            position_size=self.config['POSITION_SIZE'],
            leverage=self.config['LEVERAGE'],
            logger=self.logger
        )

        self.strategy_engine = StrategyEngine(
            data_manager=self.data_manager,
            trading_engine=self.trading_engine,
            logger=self.logger
        )

        self.running = False
        self.threads = []

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def load_config(self):
        try:
            config = {
                'SYMBOL': os.getenv('SYMBOL', 'ETHUSDT'),
                'TIMEFRAME': os.getenv('TIMEFRAME', '45m'),
                'POSITION_SIZE': float(os.getenv('POSITION_SIZE', 100)),
                'LEVERAGE': int(os.getenv('LEVERAGE', 3)),
                'BYBIT_API_KEY': os.getenv('BYBIT_API_KEY'),
                'BYBIT_SECRET': os.getenv('BYBIT_SECRET'),
                'MACD_FAST': int(os.getenv('MACD_FAST', 12)),
                'MACD_SLOW': int(os.getenv('MACD_SLOW', 26)),
                'MACD_SIGNAL': int(os.getenv('MACD_SIGNAL', 7))
            }

            if not config['BYBIT_API_KEY'] or not config['BYBIT_SECRET']:
                raise ValueError("Bybit API credentials are required")

            if config['TIMEFRAME'] not in ['5m', '45m']:
                raise ValueError("Timeframe must be '5m' or '45m'")

            self.logger.info(f"Конфигурация загружена: {config['SYMBOL']} {config['TIMEFRAME']} "
                             f"| Размер: {config['POSITION_SIZE']} USDT | Плечо: {config['LEVERAGE']}x")

            return config

        except Exception as e:
            self.logger.critical(f"Ошибка конфигурации: {e}")
            sys.exit(1)

    def signal_handler(self, signum, _):
        self.logger.info(f"Получен сигнал {signum}, завершение работы...")
        self.stop()

    def start_data_thread(self):
        def data_worker():
            try:
                self.logger.info("Загрузка исторических данных...")
                self.data_manager.get_historical_data()

                self.logger.info("Запуск WebSocket соединения...")
                self.data_manager.start_websocket()

            except Exception as e:
                self.logger.error(f"Ошибка потока данных: {e}")
                if self.running:
                    self.logger.reconnection("Менеджер данных")
                    time.sleep(10)
                    self.start_data_thread()

        thread = threading.Thread(target=data_worker, daemon=True)
        thread.start()
        self.threads.append(thread)

    def start_strategy_thread(self):
        def strategy_worker():
            try:
                while self.running:
                    macd_data = self.data_manager.get_macd_data()
                    if macd_data and 'macd' in macd_data:
                        break
                    time.sleep(1)

                if self.running:
                    self.strategy_engine.run_strategy()

            except Exception as e:
                self.logger.error(f"Strategy thread error: {e}")
                if self.running:
                    time.sleep(10)
                    self.start_strategy_thread()

        thread = threading.Thread(target=strategy_worker, daemon=True)
        thread.start()
        self.threads.append(thread)

    def start_monitor_thread(self):
        def monitor_worker():
            while self.running:
                try:
                    status = self.strategy_engine.get_status()
                    macd_data = self.data_manager.get_macd_data()
                    websocket_price = self.data_manager.get_last_websocket_price()

                    # Используем новый метод для получения информации об интервале
                    interval_info = self.data_manager.get_current_interval_info()

                    self.logger.info(
                        f"STATUS [{interval_info}] - Позиция: {status['current_position']} | "
                        f"Price: {websocket_price} | "
                        f"MACD: {macd_data.get('macd', 0):.2f} | "
                        f"Signal: {macd_data.get('signal', 0):.2f} | "
                        f"Histogram: {macd_data.get('histogram', 0):.2f} | "
                        f"Ожидание: {status['waiting_for_close']}"
                    )

                    time.sleep(60)

                except Exception as e:
                    self.logger.error(f"Monitor thread error: {e}")
                    time.sleep(60)

        thread = threading.Thread(target=monitor_worker, daemon=True)
        thread.start()
        self.threads.append(thread)

    def start(self):
        try:
            self.running = True

            self.start_data_thread()
            time.sleep(5)

            self.start_strategy_thread()
            self.start_monitor_thread()

            self.logger.info("Все системы запущены. Нажмите Ctrl+C для остановки.")

            while self.running:
                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("Shutdown initiated by user")
        except Exception as e:
            self.logger.critical(f"Critical error: {e}")
        finally:
            self.stop()

    def stop(self):
        if not self.running:
            return

        self.logger.info("Остановка торгового бота...")
        self.running = False

        if hasattr(self, 'data_manager'):
            self.data_manager.stop()

        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)

        self.logger.info("Торговый бот остановлен")


if __name__ == "__main__":
    bot = TradingBot()
    bot.start()