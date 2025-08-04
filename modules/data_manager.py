# modules/data_manager.py

import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
import json
import threading
import time
from datetime import datetime, timezone, timedelta
import websocket


class DataManager:
    def __init__(self, symbol, timeframe, logger):
        self.symbol = symbol
        self.timeframe = timeframe  # "5m" or "45m"
        self.logger = logger
        self.client = Client()
        self.ws = None

        if self.timeframe == "5m":
            # Только массив цен close
            self.klines_data = []
            self.current_data = []
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"
        else:  # 45m
            # Простая логика
            self.klines_45m = []
            self.current_45m_start = None
            self.last_45m_start = None
            self.current_45m_candle = None
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"

        self.macd_data = {}
        self.running = False
        self.lock = threading.Lock()

        # Добавляем переменную для отслеживания последней цены WebSocket
        self.last_websocket_price = 0.0

        # For interval tracking and anti-spam logging
        self.last_interval_start = None
        self.last_data_signature = None
        self.last_sync_time = None

    @staticmethod
    def get_45m_interval_start(timestamp):
        """Получить начало 45м интервала для заданного времени"""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        day_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_from_start = (timestamp - day_start).total_seconds() / 60
        interval_number = int(minutes_from_start // 45)
        interval_start = day_start + timedelta(minutes=interval_number * 45)

        return interval_start

    def get_historical_data(self, limit=200):
        try:
            if self.timeframe == "5m":

                # Получаем серверное время Binance для синхронизации
                server_time = self.client.get_server_time()
                self.logger.info(
                    f"Серверное время Binance: {pd.to_datetime(server_time['serverTime'], unit='ms')}")

                # Получаем исторические свечи с учетом серверного времени
                klines = self.client.futures_klines(
                    symbol=self.symbol,
                    interval=self.timeframe,
                    limit=limit
                )

                # Преобразуем в DataFrame
                df = pd.DataFrame(klines, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                    'taker_buy_quote_volume', 'ignore'
                ])

                # Конвертируем типы данных
                df['close'] = df['close'].astype(float)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

                # Сохраняем только цены close
                self.klines_data = df['close'].tolist()

                # Заполняем current_data для совместимости с остальным кодом
                self.current_data = df.to_dict('records')

                # Устанавливаем последнюю историческую цену как начальную WebSocket цену
                if self.klines_data:
                    self.last_websocket_price = self.klines_data[-1]

                self.last_sync_time = datetime.now()

                self.logger.info(f"Загружено {len(self.klines_data)} исторических свечей для {self.symbol}")

            else:  # 45m
                # Получаем серверное время Binance
                server_time = self.client.get_server_time()
                now = pd.to_datetime(server_time['serverTime'], unit='ms', utc=True)
                self.logger.info(f"[SYNC] Серверное время Binance: {now}")

                self.current_45m_start = DataManager.get_45m_interval_start(now)
                self.last_45m_start = self.current_45m_start

                # Запрашиваем исторические 15м свечи ФЬЮЧЕРСЫ
                klines_15m = self.client.futures_klines(
                    symbol=self.symbol,
                    interval='15m',
                    limit=limit * 3 + 10
                )

                df = pd.DataFrame(klines_15m, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                    'taker_buy_quote_volume', 'ignore'
                ])

                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = df[col].astype(float)

                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)

                # Преобразуем в 45м свечие
                self.convert_15m_to_45m(df)

                # Показываем анализ интервалов
                self.show_interval_analysis(now)

                # Доформировываем текущий интервал
                self.complete_current_interval(self.client, now)

                # Устанавливаем последнюю историческую цену как начальную WebSocket цену
                if self.klines_45m:
                    self.last_websocket_price = self.klines_45m[-1]

                self.logger.info(f"ЗАГРУЖЕНО: {len(self.klines_45m)} исторических 45м свечей")

            # Рассчитываем начальный MACD
            self.calculate_macd()

        except BinanceAPIException as api_error:
            self.logger.error(f"Binance API error: {str(api_error)}")

    def show_interval_analysis(self, now):
        """Показать анализ интервалов"""
        last_complete_interval = self.current_45m_start - timedelta(minutes=45)
        time_in_interval = (now - self.current_45m_start).total_seconds() / 60

        self.logger.info("АНАЛИЗ ИНТЕРВАЛОВ:")
        self.logger.info(f"Текущее UTC время: {now.strftime('%H:%M:%S')}")
        self.logger.info(
            f"Последний полный 45м интервал: {last_complete_interval.strftime('%H:%M')} - {(last_complete_interval + timedelta(minutes=45)).strftime('%H:%M')}")
        self.logger.info(
            f"Текущий интервал: {self.current_45m_start.strftime('%H:%M')} - {(self.current_45m_start + timedelta(minutes=45)).strftime('%H:%M')}")
        self.logger.info(f"Прошло времени в текущем интервале: {time_in_interval:.1f} минут")

    def convert_15m_to_45m(self, df_15m):
        """Преобразование 15м в 45м"""
        self.klines_45m = []
        grouped_candles = {}

        for _, row in df_15m.iterrows():
            interval_start = DataManager.get_45m_interval_start(row['timestamp'])

            if interval_start not in grouped_candles:
                grouped_candles[interval_start] = []

            grouped_candles[interval_start].append(row)

        # Формируем только полные исторические 45м свечи (исключаем текущий интервал)
        for interval_start in sorted(grouped_candles.keys()):
            candles = grouped_candles[interval_start]

            # Пропускаем текущий интервал - он будет доформирован отдельно
            if interval_start >= self.current_45m_start:
                continue

            # Пропускаем неполные интервалы
            if len(candles) < 3:
                continue

            candle_45m = {
                'timestamp': interval_start,
                'open': candles[0]['open'],
                'high': max(c['high'] for c in candles),
                'low': min(c['low'] for c in candles),
                'close': candles[-1]['close'],
                'volume': sum(c['volume'] for c in candles)
            }

            self.klines_45m.append(candle_45m['close'])

    def complete_current_interval(self, client, now):
        """Доформирование текущего 45м интервала"""
        try:
            time_in_interval = (now - self.current_45m_start).total_seconds() / 60
            completed_5m_candles = int(time_in_interval // 5)

            if completed_5m_candles == 0:
                self.logger.info(f"Дополнительные 5м свечи: 0 (первая 5м свеча ещё не завершена)")
                self.current_45m_candle = {'close': 0.0, 'high': 0.0, 'low': 999999.0, 'open': 0.0, 'volume': 0.0}
                self.klines_45m.append(0.0)
                return

            # Запрашиваем завершённые 5м свечи
            start_time = int(self.current_45m_start.timestamp() * 1000)

            klines_5m = client.futures_klines(
                symbol=self.symbol,
                interval='5m',
                startTime=start_time,
                limit=completed_5m_candles
            )

            if klines_5m:
                # Берём только нужное количество свечей
                klines_to_use = klines_5m[:completed_5m_candles]

                self.current_45m_candle = {
                    'open': float(klines_to_use[0][1]),
                    'high': max(float(k[2]) for k in klines_to_use),
                    'low': min(float(k[3]) for k in klines_to_use),
                    'close': float(klines_to_use[-1][4]),
                    'volume': sum(float(k[5]) for k in klines_to_use)
                }

                self.klines_45m.append(self.current_45m_candle['close'])

                last_completed_time = self.current_45m_start + timedelta(minutes=completed_5m_candles * 5)
                self.logger.info(f"Дополнительные 5м свечи: {len(klines_to_use)} шт (только завершённые)")
                self.logger.info(
                    f"Период завершённых данных: {self.current_45m_start.strftime('%H:%M')} - {last_completed_time.strftime('%H:%M')}")
            else:
                self.logger.info(f"Дополнительные 5м свечи: 0 (не найдены)")
                self.current_45m_candle = {'close': 0.0, 'high': 0.0, 'low': 999999.0, 'open': 0.0, 'volume': 0.0}
                self.klines_45m.append(0.0)

        except Exception as e:
            self.logger.error(f"Ошибка доформирования интервала: {e}")

    def update_current_45m_candle(self, price, high, low, volume):
        """Обновление текущей 45м свечи ТОЧНО как в тестовом модуле"""
        if self.current_45m_candle is None:
            self.current_45m_candle = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume
            }
        else:
            if self.current_45m_candle['open'] == 0.0:
                self.current_45m_candle['open'] = price

            self.current_45m_candle['high'] = max(self.current_45m_candle['high'], high)
            self.current_45m_candle['low'] = min(self.current_45m_candle['low'], low) if self.current_45m_candle['low'] != 999999.0 else low
            self.current_45m_candle['close'] = price
            self.current_45m_candle['volume'] += volume

    def check_45m_interval_change(self, timestamp):
        """Проверка смены 45м интервала"""
        current_interval = DataManager.get_45m_interval_start(timestamp)

        if current_interval != self.last_45m_start:
            # Новый 45м интервал начался!
            self.logger.info(
                f"НОВЫЙ 45М ИНТЕРВАЛ: {self.last_45m_start.strftime('%H:%M')} -> {current_interval.strftime('%H:%M')}")

            # Фиксируем предыдущую 45м свечу
            if self.current_45m_candle and self.current_45m_candle['close'] != 0.0:
                self.logger.info(f"ФИКСАЦИЯ 45М свечи: Close: {self.current_45m_candle['close']}")

            # Начинаем новую 45м свечу
            self.last_45m_start = current_interval
            self.current_45m_start = current_interval
            self.current_45m_candle = None

            # Добавляем новую пустую свечу для нового интервала
            self.klines_45m.append(0.0)

            # Ограничиваем размер массива
            if len(self.klines_45m) > 250:
                self.klines_45m = self.klines_45m[-200:]
                self.logger.info(f"Массив обрезан до {len(self.klines_45m)} свечей")

            return True
        return False

    @staticmethod
    def calculate_ema(prices, period):
        """Расчет EMA"""
        prices = np.array(prices, dtype=np.float64)

        if len(prices) < period:
            return np.full_like(prices, np.nan, dtype=np.float64)

        ema = np.full_like(prices, np.nan, dtype=np.float64)
        alpha = np.float64(2.0) / np.float64(period + 1.0)

        # Находим первый не-NaN элемент для инициализации
        first_valid = period - 1
        for i in range(len(prices)):
            if not np.isnan(prices[i]):
                if period == 7:  # Для signal EMA
                    if prices[i] != 0:
                        first_valid = max(i, period - 1)
                        break
                else:  # Для обычных EMA
                    first_valid = max(i, period - 1)
                    break

        if first_valid >= len(prices):
            return ema

        # Первое значение EMA = SMA первых period значений
        if first_valid + period <= len(prices):
            valid_window = prices[first_valid - period + 1:first_valid + 1]
            if period == 7:  # Для signal EMA
                if not np.any(np.isnan(valid_window)) and not np.any(valid_window == 0):
                    ema[first_valid] = np.mean(valid_window, dtype=np.float64)
                elif first_valid < len(prices):
                    ema[first_valid] = prices[first_valid]
            else:  # Для обычных EMA
                if not np.any(np.isnan(valid_window)):
                    ema[first_valid] = np.mean(valid_window, dtype=np.float64)
                elif first_valid < len(prices):
                    ema[first_valid] = prices[first_valid]

        # Остальные значения по формуле EMA с повышенной точностью
        for i in range(first_valid + 1, len(prices)):
            if period == 7:  # Для signal EMA
                if not np.isnan(prices[i]) and prices[i] != 0 and not np.isnan(ema[i - 1]):
                    ema[i] = alpha * prices[i] + (np.float64(1.0) - alpha) * ema[i - 1]
            else:  # Для обычных EMA
                if not np.isnan(prices[i]) and not np.isnan(ema[i - 1]):
                    ema[i] = alpha * prices[i] + (np.float64(1.0) - alpha) * ema[i - 1]

        return ema

    def calculate_macd(self):
        """Расчет MACD"""
        fast_period = 12
        slow_period = 26
        signal_period = 7

        if self.timeframe == "5m":
            if len(self.klines_data) < 32:  # 26 + 7 - 1
                return
            prices = np.array(self.klines_data, dtype=np.float64)
        else:  # 45m
            if len(self.klines_45m) < 32:
                return
            prices = np.array(self.klines_45m, dtype=np.float64)

        # Рассчитываем EMA12 и EMA26
        ema12 = DataManager.calculate_ema(prices, fast_period)
        ema26 = DataManager.calculate_ema(prices, slow_period)

        # MACD Line = EMA12 - EMA26
        macd_line = np.full_like(prices, np.nan, dtype=np.float64)

        # MACD считается только там, где есть оба EMA
        for i in range(len(prices)):
            if not np.isnan(ema12[i]) and not np.isnan(ema26[i]):
                macd_line[i] = ema12[i] - ema26[i]

        # Signal Line = EMA7 от MACD (только от валидных значений MACD)
        # Находим первый валидный MACD
        first_macd_idx = None
        for i in range(len(macd_line)):
            if not np.isnan(macd_line[i]):
                first_macd_idx = i
                break

        if first_macd_idx is None or len(macd_line) - first_macd_idx < signal_period:
            return

        # Берем только валидную часть MACD для расчета Signal
        valid_macd = macd_line[first_macd_idx:]
        signal_ema = DataManager.calculate_ema(valid_macd, signal_period)

        # Восстанавливаем Signal в полный массив
        signal_line = np.full_like(prices, np.nan, dtype=np.float64)
        signal_line[first_macd_idx:] = signal_ema

        # Получаем последние значения БЕЗ округления
        current_macd = float(macd_line[-1]) if not np.isnan(macd_line[-1]) else 0.0
        current_signal = float(signal_line[-1]) if not np.isnan(signal_line[-1]) else 0.0
        current_histogram = current_macd - current_signal

        # Проверяем что значения валидны
        if np.isnan(current_macd) or np.isnan(current_signal):
            return

        # Сохраняем значения
        self.macd_data = {
            'macd': current_macd,
            'signal': current_signal,
            'histogram': current_histogram
        }

    def handle_kline_message_5m(self, _, message):
        """Обработка WebSocket сообщений для 5m"""
        try:
            data = json.loads(message)

            if 'k' in data:
                kline = data['k']
                close_price = float(kline['c'])
                is_kline_closed = kline['x']

                # Сохраняем последнюю цену из WebSocket
                self.last_websocket_price = close_price

                if is_kline_closed:
                    # Свеча закрылась - добавляем новую свечу
                    self.klines_data.append(close_price)
                    self.logger.info(
                        f"НОВАЯ СВЕЧА: Время: {pd.to_datetime(kline['t'], unit='ms')} | Закрытие: {close_price} | Всего свечей: {len(self.klines_data)}")

                    # Ограничиваем размер массива
                    if len(self.klines_data) > 250:
                        self.klines_data = self.klines_data[-200:]
                        self.logger.info(f"Массив обрезан до {len(self.klines_data)} свечей")
                else:
                    # Свеча ещё идёт - обновляем последнюю цену
                    if len(self.klines_data) > 0:
                        self.klines_data[-1] = close_price
                    else:
                        # Первая свеча после запуска
                        self.klines_data.append(close_price)

                # Пересчитываем MACD
                self.calculate_macd()

        except Exception as ws_error:
            self.logger.error(f"Ошибка обработки WebSocket сообщения: {ws_error}")

    def handle_kline_message_45m(self, _, message):
        """Обработка WebSocket сообщений для 45m"""
        try:
            data = json.loads(message)

            if 'k' in data:
                kline = data['k']
                close_price = float(kline['c'])
                high_price = float(kline['h'])
                low_price = float(kline['l'])
                volume = float(kline['v'])
                kline_start_time = pd.to_datetime(int(kline['t']), unit='ms', utc=True)
                is_kline_closed = kline['x']

                # Сохраняем последнюю цену из WebSocket
                self.last_websocket_price = close_price

                # Проверяем смену 45м интервала
                self.check_45m_interval_change(kline_start_time)

                # Обновляем текущую 45м свечу
                self.update_current_45m_candle(close_price, high_price, low_price, volume)

                # Обновляем последнюю цену в массиве
                if len(self.klines_45m) > 0:
                    self.klines_45m[-1] = close_price

                if is_kline_closed:
                    self.logger.info(f"5М СВЕЧА ЗАКРЫТА: {kline_start_time.strftime('%H:%M')} | Цена: {close_price}")

                # Пересчитываем MACD
                self.calculate_macd()

        except Exception as e:
            self.logger.error(f"Ошибка WebSocket: {e}")

    def start_websocket(self):
        def on_message(_, message):
            if self.timeframe == "5m":
                self.handle_kline_message_5m(_, message)
            else:
                self.handle_kline_message_45m(_, message)

        def on_error(_, error):
            self.logger.error(f"Ошибка WebSocket: {error}")

        def on_close(_, __, ___):
            self.logger.info("WebSocket соединение закрыто")
            if self.running:
                time.sleep(5)
                self.start_websocket()

        def on_open(_):
            self.logger.info("FUTURES WebSocket соединение открыто")

        stream = self.ws_stream
        # Используем ФЬЮЧЕРСНЫЙ WebSocket для получения фьючерсных цен
        socket_url = f"wss://fstream.binance.com/ws/{stream}"

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        self.logger.info(f"Подключение к FUTURES WebSocket: {socket_url}")
        self.running = True
        self.ws.run_forever()

    def get_current_timeframe_status(self):
        with self.lock:
            if self.timeframe == "5m":
                if not self.current_data:
                    return None, False

                last_candle = self.current_data[-1]

                from datetime import timedelta
                current_time = datetime.now(timezone.utc)
                candle_start = last_candle['timestamp']

                if candle_start.tzinfo is None:
                    candle_start = candle_start.replace(tzinfo=timezone.utc)

                expected_end = candle_start + timedelta(minutes=5)
                is_complete = current_time >= expected_end

                return last_candle, is_complete
            else:
                if not self.current_45m_candle:
                    return None, False

                current_time = datetime.now(timezone.utc)
                candle_start = self.current_45m_start
                from datetime import timedelta
                expected_end = candle_start + timedelta(minutes=45)

                is_complete = current_time >= expected_end
                return self.current_45m_candle, is_complete

    def get_macd_data(self):
        with self.lock:
            return self.macd_data.copy()

    def get_last_websocket_price(self):
        """Получить последнюю цену из WebSocket данных"""
        with self.lock:
            return self.last_websocket_price

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()