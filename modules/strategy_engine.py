# modules/strategy_engine.py

import time
from datetime import datetime


class StrategyEngine:
    def __init__(self, data_manager, trading_engine, logger):
        self.data_manager = data_manager
        self.trading_engine = trading_engine
        self.logger = logger

        self.previous_macd = None
        self.previous_signal = None
        self.current_position_side = None
        self.last_crossover_direction = None
        self.waiting_for_candle_close = False
        self.crossover_timestamp = None

        # Новые переменные для правильной логики
        self.crossover_blocked = False  # Блокировка пересечений до конца свечи
        self.last_processed_candle_time = None  # Время последней обработанной свечи

    def detect_crossover(self, current_macd, current_signal):
        """Детекция пересечений MACD и Signal"""
        if self.previous_macd is None or self.previous_signal is None:
            return None

        # Previous state: где был MACD относительно Signal
        prev_above = self.previous_macd > self.previous_signal

        # Current state: где сейчас MACD относительно Signal
        curr_above = current_macd > current_signal

        # Detect crossover: изменение состояния
        if not prev_above and curr_above:
            # MACD был ниже Signal, теперь выше - бычье пересечение
            self.logger.info(
                f"DEBUG CROSSOVER: BULLISH detected - Prev: {self.previous_macd:.6f} < {self.previous_signal:.6f} -> Curr: {current_macd:.6f} > {current_signal:.6f}")
            return "BULLISH"
        elif prev_above and not curr_above:
            # MACD был выше Signal, теперь ниже - медвежье пересечение
            self.logger.info(
                f"DEBUG CROSSOVER: BEARISH detected - Prev: {self.previous_macd:.6f} > {self.previous_signal:.6f} -> Curr: {current_macd:.6f} < {current_signal:.6f}")
            return "BEARISH"

        return None

    def execute_crossover_action(self, direction, current_price, current_macd, current_signal):
        """Выполнение действий при пересечении"""
        if direction == "BULLISH":
            self.logger.macd_crossover("BULLISH", current_macd, current_signal, current_price)
            success = self.trading_engine.open_long()
            if success:
                self.current_position_side = "Buy"
                self.last_crossover_direction = "BULLISH"
                self.waiting_for_candle_close = True
                self.crossover_blocked = True  # Блокируем новые пересечения
                self.crossover_timestamp = datetime.now()

        elif direction == "BEARISH":
            self.logger.macd_crossover("BEARISH", current_macd, current_signal, current_price)
            success = self.trading_engine.open_short()
            if success:
                self.current_position_side = "Sell"
                self.last_crossover_direction = "BEARISH"
                self.waiting_for_candle_close = True
                self.crossover_blocked = True  # Блокируем новые пересечения
                self.crossover_timestamp = datetime.now()

    def check_crossover_maintenance(self, current_macd, current_signal):
        """Проверка сохранения направления пересечения после закрытия свечи"""
        if not self.waiting_for_candle_close or not self.last_crossover_direction:
            return

        # Check if crossover direction is maintained
        if self.last_crossover_direction == "BULLISH":
            maintained = current_macd > current_signal
        else:  # BEARISH
            maintained = current_macd < current_signal

        if maintained:
            # Crossover maintained - wait for opposite crossover
            self.logger.crossover_check(True, "WAITING_FOR_OPPOSITE")
            self.waiting_for_candle_close = False
            self.crossover_blocked = False  # Разблокируем пересечения
        else:
            # Crossover not maintained - reverse position
            self.logger.crossover_check(False, "REVERSING_POSITION")

            if self.current_position_side == "Buy":
                success = self.trading_engine.open_short()
                if success:
                    self.current_position_side = "Sell"
            else:
                success = self.trading_engine.open_long()
                if success:
                    self.current_position_side = "Buy"

            self.waiting_for_candle_close = False
            self.crossover_blocked = False  # Разблокируем пересечения
            self.last_crossover_direction = None

    def process_data_update(self):
        """Обработка обновлений данных в реальном времени"""
        macd_data = self.data_manager.get_macd_data()

        if not macd_data or 'macd' not in macd_data:
            return

        current_macd = macd_data['macd']
        current_signal = macd_data['signal']

        # Detect crossover only if we have previous data AND crossovers are not blocked
        if (self.previous_macd is not None and
                self.previous_signal is not None and
                not self.crossover_blocked):

            crossover = self.detect_crossover(current_macd, current_signal)

            if crossover:
                current_price = self.trading_engine.get_current_price()
                # Execute immediate crossover action (БЕЗ мгновенной проверки сохранения!)
                self.execute_crossover_action(crossover, current_price, current_macd, current_signal)

        # Update previous values for next iteration
        self.previous_macd = current_macd
        self.previous_signal = current_signal

    def process_candle_close(self):
        """Обработка закрытия свечи таймфрейма"""
        if not self.waiting_for_candle_close:
            return

        macd_data = self.data_manager.get_macd_data()
        if not macd_data:
            return

        current_macd = macd_data['macd']
        current_signal = macd_data['signal']

        # Check if crossover direction is maintained after candle close
        self.check_crossover_maintenance(current_macd, current_signal)

    def is_new_candle_closed(self, current_candle):
        """Проверка является ли это новой закрытой свечой"""
        if not current_candle:
            return False

        # Для 5m таймфрейма используем timestamp из current_data
        if self.data_manager.timeframe == "5m":
            if not self.data_manager.current_data:
                return False

            current_candle_time = self.data_manager.current_data[-1]['timestamp']
        else:  # 45m
            # Для 45m используем current_45m_start
            current_candle_time = self.data_manager.current_45m_start

        if current_candle_time != self.last_processed_candle_time:
            self.last_processed_candle_time = current_candle_time
            return True

        return False

    def run_strategy(self):
        """Основной цикл стратегии"""
        self.logger.info("Стратегический движок запущен")

        while True:
            try:
                # Process real-time data updates
                self.process_data_update()

                # Check if timeframe candle has closed
                current_candle, is_complete = self.data_manager.get_current_timeframe_status()

                # Обрабатываем закрытие свечи только один раз для каждой новой свечи
                if is_complete and self.waiting_for_candle_close:
                    if self.is_new_candle_closed(current_candle):
                        self.process_candle_close()

                time.sleep(1)  # Check every second

            except KeyboardInterrupt:
                self.logger.info("Стратегический движок остановлен пользователем")
                break
            except Exception as e:
                self.logger.error(f"Ошибка стратегического движка: {e}")
                time.sleep(5)
                continue

    def get_status(self):
        """Получение статуса стратегии"""
        return {
            'current_position': self.current_position_side,
            'last_crossover': self.last_crossover_direction,
            'waiting_for_close': self.waiting_for_candle_close,
            'crossover_time': self.crossover_timestamp,
            'previous_macd': self.previous_macd,
            'previous_signal': self.previous_signal,
            'crossover_blocked': self.crossover_blocked
        }