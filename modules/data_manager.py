# modules/data_manager.py

import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
import json
import threading
import time
from datetime import datetime, timezone, timedelta

# Проверяем доступность Futures WebSocket клиента
try:
    from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

    FUTURES_WS_AVAILABLE = True
except ImportError:
    UMFuturesWebsocketClient = None
    FUTURES_WS_AVAILABLE = False

import websocket


class DataManager:
    def __init__(self, symbol, timeframe, logger):
        self.symbol = symbol
        self.timeframe = timeframe  # "5m" or "45m"
        self.logger = logger
        self.client = Client()
        self.ws_client = None
        self.ws = None

        if self.timeframe == "5m":
            # ТОЧНО как в тестовом модуле - только массив цен close!
            self.klines_data = []  # Только цены close, как в тестовом модуле
            # Добавляем current_data для совместимости с остальным кодом
            self.current_data = []
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"
        else:  # 45m - ТОЧНО как в тестовом модуле
            # Как в тестовом модуле - массив цен close!
            self.klines_45m = []  # ТОЧНО как в тестовом - массив цен
            self.current_45m_start = None
            self.last_45m_start = None
            self.current_45m_candle = None
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"  # Always use 5m for real-time

        self.macd_data = {}
        self.running = False
        self.lock = threading.Lock()

        # For interval tracking and anti-spam logging
        self.last_interval_start = None
        self.last_data_signature = None
        self.last_sync_time = None

    @staticmethod
    def get_45m_interval_start(timestamp):
        """ТОЧНО как в тестовом модуле"""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        day_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_from_start = (timestamp - day_start).total_seconds() / 60
        interval_number = int(minutes_from_start // 45)
        interval_start = day_start + timedelta(minutes=interval_number * 45)

        return interval_start

    def convert_15m_to_45m(self, df_15m):
        """ТОЧНО как в тестовом модуле"""
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

            # КРИТИЧНО: Сохраняем ТОЛЬКО close цену как в тестовом модуле!
            self.klines_45m.append(candles[-1]['close'])

    def complete_current_interval(self, now):
        """ТОЧНО как в тестовом модуле"""
        try:
            time_in_interval = (now - self.current_45m_start).total_seconds() / 60
            completed_5m_candles = int(time_in_interval // 5)

            if completed_5m_candles == 0:
                # Добавляем пустую цену для текущего интервала
                self.current_45m_candle = {'close': 0.0, 'high': 0.0, 'low': 999999.0, 'open': 0.0, 'volume': 0.0}
                self.klines_45m.append(0.0)
                return

            # Запрашиваем завершённые 5м свечи
            start_time = int(self.current_45m_start.timestamp() * 1000)

            klines_5m = self.client.futures_klines(
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

                # КРИТИЧНО: Добавляем ТОЛЬКО close цену!
                self.klines_45m.append(self.current_45m_candle['close'])

            else:
                self.current_45m_candle = {'close': 0.0, 'high': 0.0, 'low': 999999.0, 'open': 0.0, 'volume': 0.0}
                self.klines_45m.append(0.0)

        except Exception as e:
            self.logger.error(f"Ошибка доформирования интервала: {e}")

    def get_historical_data(self, limit=200):
        try:
            if self.timeframe == "5m":
                # Получаем серверное время Binance для синхронизации
                server_time = self.client.get_server_time()
                self.logger.info(
                    f"[SYNC] Серверное время Binance: {pd.to_datetime(server_time['serverTime'], unit='ms')}")

                # Получаем исторические свечи с учетом серверного времени
                klines = self.client.futures_klines(
                    symbol=self.symbol,
                    interval=self.timeframe,
                    limit=limit
                )

                # Преобразуем в DataFrame ТОЧНО как в тестовом модуле
                df = pd.DataFrame(klines, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                    'taker_buy_quote_volume', 'ignore'
                ])

                # Конвертируем типы данных
                df['close'] = df['close'].astype(float)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

                # НЕ убираем последнюю свечу - она может быть закрытой
                # ТОЧНО как в тестовом модуле - сохраняем ТОЛЬКО цены close!
                self.klines_data = df['close'].tolist()

                # Заполняем current_data для совместимости с остальным кодом
                self.current_data = df.to_dict('records')

                self.last_sync_time = datetime.now()

                self.logger.info(f"Загружено {len(self.klines_data)} исторических свечей для {self.symbol}")

            else:  # 45m - ПОЛНОСТЬЮ по тестовому модулю
                # Получаем серверное время
                server_time = self.client.get_server_time()
                now = pd.to_datetime(server_time['serverTime'], unit='ms', utc=True)
                self.logger.info(f"[SYNC] Серверное время Binance: {now}")

                self.current_45m_start = DataManager.get_45m_interval_start(now)
                self.last_45m_start = self.current_45m_start

                # Запрашиваем исторические 15м свечи
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

                # Преобразуем в 45м свечи ТОЧНО как в тестовом модуле
                self.convert_15m_to_45m(df)

                # Доформировываем текущий интервал ТОЧНО как в тестовом модуле
                self.complete_current_interval(now)

                self.logger.info(f"Загружено {len(self.klines_45m)} исторических 45m свечей для {self.symbol}")

            # Рассчитываем начальный MACD
            self.calculate_macd()

        except BinanceAPIException as api_error:
            self.logger.api_error("Binance", str(api_error))

    def check_45m_interval_change(self, timestamp):
        """ТОЧНО как в тестовом модуле"""
        current_interval = DataManager.get_45m_interval_start(timestamp)

        if current_interval != self.last_45m_start:
            # Новый 45м интервал начался!
            self.logger.info(
                f"[НОВЫЙ 45М ИНТЕРВАЛ] {self.last_45m_start.strftime('%H:%M')} -> {current_interval.strftime('%H:%M')}")

            # Фиксируем предыдущую 45м свечу
            if self.current_45m_candle and self.current_45m_candle['close'] != 0.0:
                self.logger.info(f"[ФИКСАЦИЯ 45М] Close: {self.current_45m_candle['close']}")

            # Начинаем новую 45м свечу
            self.last_45m_start = current_interval
            self.current_45m_start = current_interval
            self.current_45m_candle = None

            # Добавляем новую пустую свечу для нового интервала
            self.klines_45m.append(0.0)

            # Ограничиваем размер массива
            if len(self.klines_45m) > 250:
                self.klines_45m = self.klines_45m[-200:]
                self.logger.info(f"[ОБРЕЗКА] Массив обрезан до {len(self.klines_45m)} свечей")

            return True
        return False

    def update_current_45m_candle(self, price, high, low, volume):
        """ТОЧНО как в тестовом модуле"""
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
            self.current_45m_candle['low'] = min(self.current_45m_candle['low'], low) if self.current_45m_candle[
                                                                                             'low'] != 999999.0 else low
            self.current_45m_candle['close'] = price
            self.current_45m_candle['volume'] += volume

    @staticmethod
    def create_45m_candle(candles_15m, start_time, is_current=False):
        if not candles_15m:
            return None

        return {
            'timestamp': start_time,
            'open': candles_15m[0]['open'],
            'high': max([c['high'] for c in candles_15m]),
            'low': min([c['low'] for c in candles_15m]),
            'close': candles_15m[-1]['close'],
            'volume': sum([c['volume'] for c in candles_15m]),
            'is_current': is_current
        }

    @staticmethod
    def calculate_ema(prices, period):
        """ТОЧНО как в тестовом модуле с проверкой на нули"""
        prices = np.array(prices, dtype=np.float64)

        if len(prices) < period:
            return np.full_like(prices, np.nan, dtype=np.float64)

        ema = np.full_like(prices, np.nan, dtype=np.float64)
        alpha = np.float64(2.0) / np.float64(period + 1.0)

        # КРИТИЧНО: Находим первый не-NaN и НЕ-НУЛЕВОЙ элемент!
        first_valid = period - 1
        for i in range(len(prices)):
            if not np.isnan(prices[i]) and prices[i] != 0:  # ДОБАВЛЕНА ПРОВЕРКА НА НОЛЬ!
                first_valid = max(i, period - 1)
                break

        if first_valid >= len(prices):
            return ema

        # Первое значение EMA = SMA первых period значений
        if first_valid + period <= len(prices):
            valid_window = prices[first_valid - period + 1:first_valid + 1]
            if not np.any(np.isnan(valid_window)) and not np.any(valid_window == 0):  # ПРОВЕРКА НА НУЛИ!
                ema[first_valid] = np.mean(valid_window, dtype=np.float64)
            elif first_valid < len(prices):
                ema[first_valid] = prices[first_valid]

        # Остальные значения по формуле EMA с повышенной точностью
        for i in range(first_valid + 1, len(prices)):
            if not np.isnan(prices[i]) and prices[i] != 0 and not np.isnan(ema[i - 1]):  # ПРОВЕРКА НА НУЛИ!
                ema[i] = alpha * prices[i] + (np.float64(1.0) - alpha) * ema[i - 1]

        return ema

    def calculate_macd(self):
        """Расчет MACD ТОЧНО как в тестовом модуле"""
        # Используем параметры MACD из тестового модуля
        fast_period = 12
        slow_period = 26
        signal_period = 7

        if self.timeframe == "5m":
            if len(self.klines_data) < 32:  # 26 + 7 - 1
                return
            prices = np.array(self.klines_data, dtype=np.float64)  # ПРЯМО массив цен!
        else:  # 45m - ТОЧНО как в тестовом модуле
            if len(self.klines_45m) < 32:
                return
            prices = np.array(self.klines_45m, dtype=np.float64)  # ПРЯМО массив цен!

        # Рассчитываем EMA12 и EMA26 с максимальной точностью
        ema12 = DataManager.calculate_ema(prices, fast_period)
        ema26 = DataManager.calculate_ema(prices, slow_period)

        # MACD Line = EMA12 - EMA26 (без округления)
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

        # Сохраняем значения БЕЗ промежуточного округления
        self.macd_data = {
            'macd': current_macd,
            'signal': current_signal,
            'histogram': current_histogram
        }

    def handle_kline_message_5m(self, _, message):
        """Обработка WebSocket сообщений ТОЧНО как в тестовом модуле для 5m"""
        try:
            data = json.loads(message)

            if 'k' in data:
                kline = data['k']
                close_price = float(kline['c'])
                is_kline_closed = kline['x']  # True если свеча закрылась

                if is_kline_closed:
                    # Свеча закрылась - добавляем новую свечу с этой ценой закрытия
                    self.klines_data.append(close_price)
                    self.logger.info(
                        f"[НОВАЯ СВЕЧА] Время: {pd.to_datetime(kline['t'], unit='ms')} | Закрытие: {close_price} | Всего свечей: {len(self.klines_data)}")

                    # Ограничиваем размер массива для производительности
                    if len(self.klines_data) > 250:
                        self.klines_data = self.klines_data[-200:]
                        self.logger.info(f"[ОБРЕЗКА] Массив обрезан до {len(self.klines_data)} свечей")
                else:
                    # Свеча ещё идёт - обновляем последнюю цену
                    if len(self.klines_data) > 0:
                        self.klines_data[-1] = close_price
                    else:
                        # Первая свеча после запуска
                        self.klines_data.append(close_price)

                # Пересчитываем MACD с каждым обновлением
                self.calculate_macd()

        except Exception as ws_error:
            self.logger.error(f"Ошибка обработки WebSocket сообщения: {ws_error}")

    def handle_kline_message_45m(self, _, message):
        """Обработка WebSocket сообщений ТОЧНО как в тестовом модуле для 45m"""
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

                # Проверяем смену 45м интервала
                self.check_45m_interval_change(kline_start_time)

                # Обновляем текущую 45м свечу
                self.update_current_45m_candle(close_price, high_price, low_price, volume)

                # КРИТИЧНО: Обновляем последнюю цену в массиве как в тестовом модуле
                if len(self.klines_45m) > 0:
                    self.klines_45m[-1] = close_price

                if is_kline_closed:
                    self.logger.info(f"[5М СВЕЧА ЗАКРЫТА] {kline_start_time.strftime('%H:%M')} | Цена: {close_price}")

                # Пересчитываем MACD
                self.calculate_macd()

        except Exception as ws_error:
            self.logger.error(f"Ошибка обработки WebSocket сообщения: {ws_error}")

    def start_websocket(self):
        # Пытаемся использовать Futures WebSocket если доступен
        if FUTURES_WS_AVAILABLE and UMFuturesWebsocketClient is not None:
            try:
                if self.timeframe == "5m":
                    self.ws_client = UMFuturesWebsocketClient(
                        on_message=self.handle_kline_message_5m
                    )
                    self.ws_client.kline(symbol=self.symbol.lower(), interval=self.timeframe)
                    self.logger.info(f"WebSocket подключен для {self.symbol} {self.timeframe} (Futures)")
                else:  # 45m
                    self.ws_client = UMFuturesWebsocketClient(
                        on_message=self.handle_kline_message_45m
                    )
                    # КРИТИЧНО: Подписываемся на 5m данные для агрегации в 45m
                    self.ws_client.kline(symbol=self.symbol.lower(), interval='5m')
                    self.logger.info(f"WebSocket подключен для {self.symbol} 5m (для 45m агрегации)")

                self.running = True
                return

            except Exception as futures_error:
                self.logger.error(f"Ошибка Futures WebSocket подключения: {futures_error}")

        # Fallback to regular websocket
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
            self.logger.info("WebSocket соединение открыто")

        stream = self.ws_stream
        socket_url = f"wss://fstream.binance.com/ws/{stream}"

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        self.running = True
        self.ws.run_forever()

    def get_current_timeframe_status(self):
        with self.lock:
            if self.timeframe == "5m":
                if not self.current_data:
                    return None, False

                # Return last 5m candle and its completion status
                last_candle = self.current_data[-1]

                # For 5m, we can check if current time has passed the candle's expected end
                current_time = datetime.now(timezone.utc)
                candle_start = last_candle['timestamp']

                # Ensure timezone-aware comparison
                if candle_start.tzinfo is None:
                    candle_start = candle_start.replace(tzinfo=timezone.utc)

                expected_end = candle_start + timedelta(minutes=5)
                is_complete = current_time >= expected_end

                return last_candle, is_complete
            else:
                # 45m logic - check if 45m interval completed
                if not self.current_45m_candle or not self.current_45m_start:
                    return None, False

                current_time = datetime.now(timezone.utc)
                expected_end = self.current_45m_start + timedelta(minutes=45)
                is_complete = current_time >= expected_end

                # Возвращаем совместимый формат
                fake_candle = {
                    'timestamp': self.current_45m_start,
                    'close': self.current_45m_candle['close']
                }
                return fake_candle, is_complete

    def get_macd_data(self):
        with self.lock:
            return self.macd_data.copy()

    def stop(self):
        self.running = False
        if self.ws_client:
            self.ws_client.stop()
        if self.ws:
            self.ws.close()