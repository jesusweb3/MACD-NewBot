# modules/strategy_engine.py

import time
from datetime import datetime
from enum import Enum


class StrategyState(Enum):
    WAITING_FIRST_CROSS = "waiting_first_cross"
    BLOCKED_UNTIL_CLOSE = "blocked_until_close"
    WAITING_OPPOSITE_CROSS = "waiting_opposite_cross"


class StrategyEngine:
    def __init__(self, data_manager, trading_engine, logger):
        self.data_manager = data_manager
        self.trading_engine = trading_engine
        self.logger = logger

        # Текущее состояние стратегии
        self.state = StrategyState.WAITING_FIRST_CROSS

        # Данные о текущей позиции
        self.current_position_side = None  # "Buy" или "Sell"
        self.position_direction = None  # "BULLISH" или "BEARISH"

        # Данные о пересечении
        self.crossover_interval_start = None
        self.crossover_timestamp = None

        # Предыдущие значения MACD для детекции пересечений
        self.previous_macd = None
        self.previous_signal = None

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
            return "BULLISH"  # MACD пересекла Signal снизу вверх
        elif prev_above and not curr_above:
            return "BEARISH"  # MACD пересекла Signal сверху вниз

        return None

    def execute_position(self, direction, current_price, current_macd, current_signal):
        """Открытие позиции по направлению пересечения"""
        if direction == "BULLISH":
            self.logger.macd_crossover("BULLISH", current_macd, current_signal, current_price)
            success = self.trading_engine.open_long()
            if success:
                self.current_position_side = "Buy"
                self.position_direction = "BULLISH"
                return True

        elif direction == "BEARISH":
            self.logger.macd_crossover("BEARISH", current_macd, current_signal, current_price)
            success = self.trading_engine.open_short()
            if success:
                self.current_position_side = "Sell"
                self.position_direction = "BEARISH"
                return True

        return False

    def handle_first_crossover(self, direction, current_price, current_macd, current_signal):
        """Обработка первого пересечения в интервале"""
        current_interval = self.get_current_interval_start()

        self.logger.info(f"ПЕРВОЕ ПЕРЕСЕЧЕНИЕ в интервале: {direction}")

        if self.execute_position(direction, current_price, current_macd, current_signal):
            # Переходим в состояние блокировки до конца интервала
            self.state = StrategyState.BLOCKED_UNTIL_CLOSE
            self.crossover_interval_start = current_interval
            self.crossover_timestamp = datetime.now()

            self.logger.info(f"БЛОКИРОВКА до конца интервала {current_interval}")

    def handle_opposite_crossover(self, direction, current_price, current_macd, current_signal):
        """Обработка обратного пересечения"""
        if not self.position_direction:
            return False

        # Проверяем что это действительно обратное пересечение
        is_opposite = ((self.position_direction == "BULLISH" and direction == "BEARISH") or
                       (self.position_direction == "BEARISH" and direction == "BULLISH"))

        if is_opposite:
            current_interval = self.get_current_interval_start()
            self.logger.info(f"ОБРАТНОЕ ПЕРЕСЕЧЕНИЕ: {self.position_direction} -> {direction}")

            if self.execute_position(direction, current_price, current_macd, current_signal):
                # Переходим в состояние блокировки до конца нового интервала
                self.state = StrategyState.BLOCKED_UNTIL_CLOSE
                self.crossover_interval_start = current_interval
                self.crossover_timestamp = datetime.now()

                self.logger.info(f"БЛОКИРОВКА до конца интервала {current_interval}")
                return True

        return False

    def check_crossover_preserved(self, current_macd, current_signal):
        """Проверка сохранилось ли пересечение после закрытия интервала"""
        if not self.position_direction:
            return False

        if self.position_direction == "BULLISH":
            return current_macd > current_signal
        else:  # BEARISH
            return current_macd < current_signal

    def handle_interval_close(self, current_price, current_macd, current_signal):
        """Обработка закрытия интервала - проверка сохранения пересечения"""
        crossover_preserved = self.check_crossover_preserved(current_macd, current_signal)

        self.logger.info(f"ПРОВЕРКА после закрытия интервала:")
        self.logger.info(f"Направление позиции: {self.position_direction}")
        self.logger.info(f"MACD: {current_macd:.6f} | Signal: {current_signal:.6f}")
        self.logger.info(f"Пересечение сохранилось: {crossover_preserved}")

        if crossover_preserved:
            # Пересечение сохранилось - ждем обратного пересечения
            self.logger.crossover_check(True, "WAITING_FOR_OPPOSITE")
            self.state = StrategyState.WAITING_OPPOSITE_CROSS

        else:
            # Пересечение не сохранилось - разворачиваем позицию
            self.logger.crossover_check(False, "REVERSING_POSITION")

            # Определяем новое направление (противоположное текущему)
            new_direction = "BEARISH" if self.position_direction == "BULLISH" else "BULLISH"

            if self.execute_position(new_direction, current_price, current_macd, current_signal):
                # После разворота ждем обратного пересечения
                self.state = StrategyState.WAITING_OPPOSITE_CROSS
                self.logger.info(f"ПОЗИЦИЯ РАЗВЕРНУТА на {new_direction} | Ждем обратного пересечения")

    def has_interval_changed(self):
        """Проверка изменился ли интервал с момента пересечения"""
        if not self.crossover_interval_start:
            return False

        current_interval = self.get_current_interval_start()
        return current_interval != self.crossover_interval_start

    def process_data_update(self):
        """Основная логика обработки обновлений данных"""
        macd_data = self.data_manager.get_macd_data()
        if not macd_data or 'macd' not in macd_data:
            return

        current_macd = macd_data['macd']
        current_signal = macd_data['signal']
        current_price = self.trading_engine.get_current_price()

        # Проверяем смену интервала для состояния BLOCKED_UNTIL_CLOSE
        if self.state == StrategyState.BLOCKED_UNTIL_CLOSE:
            if self.has_interval_changed():
                self.logger.info(
                    f"ИНТЕРВАЛ ЗАКРЫЛСЯ: {self.crossover_interval_start} -> {self.get_current_interval_start()}")
                self.handle_interval_close(current_price, current_macd, current_signal)

        # Обрабатываем пересечения только если не заблокированы
        elif self.previous_macd is not None and self.previous_signal is not None:
            crossover = self.detect_crossover(current_macd, current_signal)

            if crossover:
                if self.state == StrategyState.WAITING_FIRST_CROSS:
                    # Первое пересечение в интервале
                    self.handle_first_crossover(crossover, current_price, current_macd, current_signal)

                elif self.state == StrategyState.WAITING_OPPOSITE_CROSS:
                    # Обратное пересечение
                    self.handle_opposite_crossover(crossover, current_price, current_macd, current_signal)

        # Обновляем предыдущие значения для следующей итерации
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
            'state': self.state.value,
            'current_position': self.current_position_side,
            'position_direction': self.position_direction,
            'crossover_interval': self.crossover_interval_start,
            'crossover_time': self.crossover_timestamp,
            'previous_macd': self.previous_macd,
            'previous_signal': self.previous_signal
        }