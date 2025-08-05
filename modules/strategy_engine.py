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

        # Переменные для правильной логики по интервалам
        self.active_crossover_direction = None  # Направление активного пересечения
        self.crossover_interval_start = None  # Интервал когда произошло пересечение
        self.waiting_for_interval_close = False  # Ждем закрытия интервала для проверки
        self.trading_blocked = False  # Блокировка торговли до конца интервала
        self.crossover_timestamp = None

    def get_current_interval_start(self):
        """Получить начало текущего интервала"""
        if self.data_manager.timeframe == "5m":
            return self.data_manager.current_interval_start
        else:  # 45m
            return self.data_manager.current_45m_start

    def detect_crossover(self, current_macd, current_signal):
        """Детекция пересечений MACD и Signal"""
        if self.previous_macd is None or self.previous_signal is None:
            return None

        prev_above = self.previous_macd > self.previous_signal
        curr_above = current_macd > current_signal

        if not prev_above and curr_above:
            self.logger.info(
                f"DEBUG CROSSOVER: BULLISH detected - Prev: {self.previous_macd:.6f} < {self.previous_signal:.6f} -> Curr: {current_macd:.6f} > {current_signal:.6f}")
            return "BULLISH"
        elif prev_above and not curr_above:
            self.logger.info(
                f"DEBUG CROSSOVER: BEARISH detected - Prev: {self.previous_macd:.6f} > {self.previous_signal:.6f} -> Curr: {current_macd:.6f} < {current_signal:.6f}")
            return "BEARISH"

        return None

    def execute_first_crossover(self, direction, current_price, current_macd, current_signal):
        """Выполнение первого пересечения в интервале"""
        current_interval = self.get_current_interval_start()

        self.logger.info(f"ПЕРВОЕ ПЕРЕСЕЧЕНИЕ в интервале {current_interval}: {direction}")

        if direction == "BULLISH":
            self.logger.macd_crossover("BULLISH", current_macd, current_signal, current_price)
            success = self.trading_engine.open_long()
            if success:
                self.current_position_side = "Buy"

        elif direction == "BEARISH":
            self.logger.macd_crossover("BEARISH", current_macd, current_signal, current_price)
            success = self.trading_engine.open_short()
            if success:
                self.current_position_side = "Sell"

        # Устанавливаем блокировку до конца интервала
        self.active_crossover_direction = direction
        self.crossover_interval_start = current_interval
        self.waiting_for_interval_close = True
        self.trading_blocked = True
        self.crossover_timestamp = datetime.now()

        self.logger.info(f"ТОРГОВЛЯ ЗАБЛОКИРОВАНА до конца интервала")

    def execute_opposite_crossover(self, direction, current_price, current_macd, current_signal):
        """Выполнение обратного пересечения"""
        if not self.active_crossover_direction:
            return False

        # Проверяем что это обратное пересечение
        is_opposite = ((self.active_crossover_direction == "BULLISH" and direction == "BEARISH") or
                       (self.active_crossover_direction == "BEARISH" and direction == "BULLISH"))

        if is_opposite:
            current_interval = self.get_current_interval_start()
            self.logger.info(f"ОБРАТНОЕ ПЕРЕСЕЧЕНИЕ: {self.active_crossover_direction} -> {direction}")

            if direction == "BULLISH":
                self.logger.macd_crossover("BULLISH", current_macd, current_signal, current_price)
                success = self.trading_engine.open_long()
                if success:
                    self.current_position_side = "Buy"

            elif direction == "BEARISH":
                self.logger.macd_crossover("BEARISH", current_macd, current_signal, current_price)
                success = self.trading_engine.open_short()
                if success:
                    self.current_position_side = "Sell"

            # Блокируем торговлю до конца этого интервала
            self.active_crossover_direction = direction
            self.crossover_interval_start = current_interval
            self.waiting_for_interval_close = True
            self.trading_blocked = True
            self.crossover_timestamp = datetime.now()

            self.logger.info(f"ТОРГОВЛЯ ЗАБЛОКИРОВАНА до конца интервала")
            return True

        return False

    def check_interval_change(self):
        """Проверка смены интервала"""
        current_interval = self.get_current_interval_start()

        if (self.waiting_for_interval_close and
                self.crossover_interval_start and
                current_interval != self.crossover_interval_start):
            self.logger.info(f"ИНТЕРВАЛ ЗАКРЫЛСЯ: {self.crossover_interval_start} -> {current_interval}")
            return True

        return False

    def process_interval_close(self, current_macd, current_signal):
        """Обработка закрытия интервала - проверка сохранения пересечения"""
        if not self.waiting_for_interval_close or not self.active_crossover_direction:
            return

        # Проверяем сохранилось ли направление пересечения
        if self.active_crossover_direction == "BULLISH":
            crossover_maintained = current_macd > current_signal
        else:  # BEARISH
            crossover_maintained = current_macd < current_signal

        self.logger.info(f"ПРОВЕРКА после закрытия интервала:")
        self.logger.info(f"Пересечение: {self.active_crossover_direction}")
        self.logger.info(f"MACD: {current_macd:.6f} | Signal: {current_signal:.6f}")
        self.logger.info(f"Направление сохранилось: {crossover_maintained}")

        if crossover_maintained:
            # Пересечение сохранилось - ждем обратного пересечения
            self.logger.crossover_check(True, "WAITING_FOR_OPPOSITE")
            self.waiting_for_interval_close = False
            self.trading_blocked = False

        else:
            # Пересечение не сохранилось - разворачиваем позицию
            self.logger.crossover_check(False, "REVERSING_POSITION")

            new_direction = None

            if self.current_position_side == "Buy":
                success = self.trading_engine.open_short()
                if success:
                    self.current_position_side = "Sell"
                    new_direction = "BEARISH"
            else:
                success = self.trading_engine.open_long()
                if success:
                    self.current_position_side = "Buy"
                    new_direction = "BULLISH"

            # После разворота ждем обратного пересечения (НЕ блокируем)
            if new_direction:
                self.active_crossover_direction = new_direction
                self.waiting_for_interval_close = False
                self.trading_blocked = False

                self.logger.info(f"НОВАЯ ПОЗИЦИЯ: {new_direction} | Ждем обратного пересечения")

    def process_data_update(self):
        """Обработка обновлений данных"""
        macd_data = self.data_manager.get_macd_data()
        if not macd_data or 'macd' not in macd_data:
            return

        current_macd = macd_data['macd']
        current_signal = macd_data['signal']

        # Проверяем смену интервала
        if self.check_interval_change():
            self.process_interval_close(current_macd, current_signal)

        # Обрабатываем пересечения только если торговля не заблокирована
        if (not self.trading_blocked and
                self.previous_macd is not None and
                self.previous_signal is not None):

            crossover = self.detect_crossover(current_macd, current_signal)

            if crossover:
                current_price = self.trading_engine.get_current_price()

                if self.active_crossover_direction is None:
                    # Первое пересечение в интервале
                    self.execute_first_crossover(crossover, current_price, current_macd, current_signal)
                else:
                    # Проверяем обратное пересечение
                    self.execute_opposite_crossover(crossover, current_price, current_macd, current_signal)

        # Обновляем предыдущие значения
        self.previous_macd = current_macd
        self.previous_signal = current_signal

    def run_strategy(self):
        """Основной цикл стратегии"""
        self.logger.info("Стратегический движок запущен")

        while True:
            try:
                self.process_data_update()
                time.sleep(1)

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
            'active_crossover': self.active_crossover_direction,
            'waiting_for_close': self.waiting_for_interval_close,
            'trading_blocked': self.trading_blocked,
            'crossover_interval': self.crossover_interval_start,
            'crossover_time': self.crossover_timestamp,
            'previous_macd': self.previous_macd,
            'previous_signal': self.previous_signal
        }