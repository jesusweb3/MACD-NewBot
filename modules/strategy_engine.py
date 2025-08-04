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

    def detect_crossover(self, current_macd, current_signal):
        if self.previous_macd is None or self.previous_signal is None:
            return None

        # Previous state
        prev_above = self.previous_macd > self.previous_signal

        # Current state
        curr_above = current_macd > current_signal

        # Detect crossover
        if not prev_above and curr_above:
            return "BULLISH"  # MACD crossed above Signal
        elif prev_above and not curr_above:
            return "BEARISH"  # MACD crossed below Signal

        return None

    def execute_crossover_action(self, direction, current_price):
        if direction == "BULLISH":
            self.logger.macd_crossover("BULLISH", self.previous_macd, self.previous_signal, current_price)
            success = self.trading_engine.open_long()
            if success:
                self.current_position_side = "Buy"
                self.last_crossover_direction = "BULLISH"
                self.waiting_for_candle_close = True
                self.crossover_timestamp = datetime.now()

        elif direction == "BEARISH":
            self.logger.macd_crossover("BEARISH", self.previous_macd, self.previous_signal, current_price)
            success = self.trading_engine.open_short()
            if success:
                self.current_position_side = "Sell"
                self.last_crossover_direction = "BEARISH"
                self.waiting_for_candle_close = True
                self.crossover_timestamp = datetime.now()

    def check_crossover_maintenance(self, current_macd, current_signal):
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
            self.last_crossover_direction = None

    def process_data_update(self):
        macd_data = self.data_manager.get_macd_data()

        if not macd_data or 'macd' not in macd_data:
            return

        current_macd = macd_data['macd']
        current_signal = macd_data['signal']
        current_price = self.trading_engine.get_current_price()

        # Detect crossover only if we have previous data
        if self.previous_macd is not None and self.previous_signal is not None:
            crossover = self.detect_crossover(current_macd, current_signal)

            if crossover and not self.waiting_for_candle_close:
                # Execute immediate crossover action
                self.execute_crossover_action(crossover, current_price)

        # Update previous values for next iteration
        self.previous_macd = current_macd
        self.previous_signal = current_signal

    def process_candle_close(self):
        if not self.waiting_for_candle_close:
            return

        macd_data = self.data_manager.get_macd_data()
        if not macd_data:
            return

        current_macd = macd_data['macd']
        current_signal = macd_data['signal']

        # Check if crossover direction is maintained after candle close
        self.check_crossover_maintenance(current_macd, current_signal)

    def run_strategy(self):
        self.logger.info("Стратегический движок запущен")

        while True:
            try:
                # Process real-time data updates
                self.process_data_update()

                # Check if timeframe candle has closed
                current_candle, is_complete = self.data_manager.get_current_timeframe_status()

                if is_complete and self.waiting_for_candle_close:
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
        return {
            'current_position': self.current_position_side,
            'last_crossover': self.last_crossover_direction,
            'waiting_for_close': self.waiting_for_candle_close,
            'crossover_time': self.crossover_timestamp,
            'previous_macd': self.previous_macd,
            'previous_signal': self.previous_signal
        }