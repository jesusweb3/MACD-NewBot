# modules/data_manager.py

import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
import json
import threading
import time
from datetime import timezone, timedelta
import websocket


class DataManager:
    def __init__(self, symbol, timeframe, logger, config):
        self.symbol = symbol
        self.timeframe = timeframe
        self.logger = logger
        self.config = config
        self.client = Client()
        self.ws = None

        # Общие данные
        self.macd_data = {}
        self.running = False
        self.lock = threading.Lock()
        self.last_websocket_price = 0.0

        if self.timeframe == "5m":
            # 5м данные
            self.klines_data = []
            self.current_interval_start = None
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"
        else:  # 45m
            # 45м данные
            self.klines_45m = []
            self.current_45m_start = None
            self.current_45m_candle = None
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"  # Строим из 5м

    @staticmethod
    def get_45m_interval_start(timestamp):
        """Получить начало 45м интервала"""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        day_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_from_start = (timestamp - day_start).total_seconds() / 60
        interval_number = int(minutes_from_start // 45)
        interval_start = day_start + timedelta(minutes=interval_number * 45)

        return interval_start

    @staticmethod
    def get_5m_interval_start(timestamp):
        """Получить начало 5м интервала"""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        day_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_from_start = (timestamp - day_start).total_seconds() / 60
        interval_number = int(minutes_from_start // 5)
        interval_start = day_start + timedelta(minutes=interval_number * 5)

        return interval_start

    @staticmethod
    def to_msk_time(utc_timestamp):
        """Конвертация UTC в МСК"""
        if utc_timestamp.tzinfo is None:
            utc_timestamp = utc_timestamp.replace(tzinfo=timezone.utc)
        return utc_timestamp + timedelta(hours=3)

    def get_historical_data(self, limit=200):
        try:
            server_time = self.client.get_server_time()
            server_dt = pd.to_datetime(server_time['serverTime'], unit='ms', utc=True)
            server_msk = DataManager.to_msk_time(server_dt)
            self.logger.info(f"Серверное время Binance: {server_msk.strftime('%Y-%m-%d %H:%M:%S')} МСК")

            if self.timeframe == "5m":
                klines = self.client.futures_klines(
                    symbol=self.symbol,
                    interval=self.timeframe,
                    limit=limit
                )

                df = pd.DataFrame(klines, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                    'taker_buy_quote_volume', 'ignore'
                ])

                df['close'] = df['close'].astype(float)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)

                self.klines_data = df['close'].tolist()
                self.current_interval_start = DataManager.get_5m_interval_start(server_dt)

                if self.klines_data:
                    self.last_websocket_price = self.klines_data[-1]

                self.logger.info(f"Загружено {len(self.klines_data)} исторических 5м свечей")

            else:  # 45m
                self.current_45m_start = DataManager.get_45m_interval_start(server_dt)

                # Получаем 15м данные для построения 45м
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

                self.convert_15m_to_45m(df)
                self.complete_current_45m_interval(server_dt)

                if self.klines_45m:
                    self.last_websocket_price = self.klines_45m[-1]

                self.logger.info(f"Загружено {len(self.klines_45m)} исторических 45м свечей")

            self.calculate_macd()

        except BinanceAPIException as e:
            self.logger.error(f"Binance API error: {str(e)}")

    def convert_15m_to_45m(self, df_15m):
        """Конвертация 15м в 45м свечи"""
        self.klines_45m = []
        grouped_candles = {}

        for _, row in df_15m.iterrows():
            interval_start = DataManager.get_45m_interval_start(row['timestamp'])

            if interval_start not in grouped_candles:
                grouped_candles[interval_start] = []

            grouped_candles[interval_start].append(row)

        # Создаем только полные исторические интервалы (исключаем текущий)
        for interval_start in sorted(grouped_candles.keys()):
            candles = grouped_candles[interval_start]

            if interval_start >= self.current_45m_start or len(candles) < 3:
                continue

            self.klines_45m.append(candles[-1]['close'])

    def complete_current_45m_interval(self, server_dt):
        """Достроить текущий 45м интервал из 5м данных"""
        try:
            time_in_interval = (server_dt - self.current_45m_start).total_seconds() / 60
            completed_5m_candles = int(time_in_interval // 5)

            if completed_5m_candles <= 0:
                self.current_45m_candle = {'open': 0.0, 'high': 0.0, 'low': 999999.0, 'close': 0.0, 'volume': 0.0}
                self.klines_45m.append(0.0)
                return

            start_time = int(self.current_45m_start.timestamp() * 1000)

            klines_5m = self.client.futures_klines(
                symbol=self.symbol,
                interval='5m',
                startTime=start_time,
                limit=completed_5m_candles
            )

            if klines_5m:
                klines_to_use = klines_5m[:completed_5m_candles]

                self.current_45m_candle = {
                    'open': float(klines_to_use[0][1]),
                    'high': max(float(k[2]) for k in klines_to_use),
                    'low': min(float(k[3]) for k in klines_to_use),
                    'close': float(klines_to_use[-1][4]),
                    'volume': sum(float(k[5]) for k in klines_to_use)
                }

                self.klines_45m.append(self.current_45m_candle['close'])

                self.logger.info(f"Текущий 45м интервал дострое из {len(klines_to_use)} завершенных 5м свечей")
            else:
                self.current_45m_candle = {'open': 0.0, 'high': 0.0, 'low': 999999.0, 'close': 0.0, 'volume': 0.0}
                self.klines_45m.append(0.0)

        except Exception as e:
            self.logger.error(f"Ошибка достройки 45м интервала: {e}")

    def update_45m_candle(self, price, high, low, volume):
        """Обновление текущей 45м свечи"""
        if self.current_45m_candle is None:
            self.current_45m_candle = {
                'open': price, 'high': price, 'low': price,
                'close': price, 'volume': volume
            }
        else:
            if self.current_45m_candle['open'] == 0.0:
                self.current_45m_candle['open'] = price

            self.current_45m_candle['high'] = max(self.current_45m_candle['high'], high)
            self.current_45m_candle['low'] = min(self.current_45m_candle['low'], low) if self.current_45m_candle[
                                                                                             'low'] != 999999.0 else low
            self.current_45m_candle['close'] = price
            self.current_45m_candle['volume'] += volume

    def handle_5m_websocket(self, message):
        """WebSocket обработка для 5м таймфрейма"""
        try:
            data = json.loads(message)
            if 'k' not in data:
                return

            kline = data['k']
            close_price = float(kline['c'])
            kline_start_time = pd.to_datetime(int(kline['t']), unit='ms', utc=True)
            is_kline_closed = kline['x']

            self.last_websocket_price = close_price

            # Проверяем смену интервала
            new_interval = DataManager.get_5m_interval_start(kline_start_time)
            interval_changed = (new_interval != self.current_interval_start)

            if is_kline_closed:
                if interval_changed:
                    # Новый интервал начался
                    old_msk = DataManager.to_msk_time(self.current_interval_start)
                    new_msk = DataManager.to_msk_time(new_interval)

                    self.logger.info(
                        f"5М ИНТЕРВАЛ ЗАКРЫТ: {old_msk.strftime('%H:%M')}-{(old_msk + timedelta(minutes=5)).strftime('%H:%M')} МСК | Close: {close_price}")
                    self.logger.info(
                        f"5М ИНТЕРВАЛ ОТКРЫТ: {new_msk.strftime('%H:%M')}-{(new_msk + timedelta(minutes=5)).strftime('%H:%M')} МСК")

                    self.current_interval_start = new_interval

                # Добавляем новую свечу
                self.klines_data.append(close_price)

                # Ограничиваем размер
                if len(self.klines_data) > 250:
                    self.klines_data = self.klines_data[-200:]
            else:
                # Обновляем текущую свечу
                if len(self.klines_data) > 0:
                    self.klines_data[-1] = close_price
                else:
                    self.klines_data.append(close_price)

            self.calculate_macd()

        except Exception as e:
            self.logger.error(f"Ошибка 5м WebSocket: {e}")

    def handle_45m_websocket(self, message):
        """WebSocket обработка для 45м таймфрейма"""
        try:
            data = json.loads(message)
            if 'k' not in data:
                return

            kline = data['k']
            close_price = float(kline['c'])
            high_price = float(kline['h'])
            low_price = float(kline['l'])
            volume = float(kline['v'])
            kline_start_time = pd.to_datetime(int(kline['t']), unit='ms', utc=True)

            self.last_websocket_price = close_price

            # Проверяем смену 45м интервала
            new_45m_interval = DataManager.get_45m_interval_start(kline_start_time)
            interval_changed = (new_45m_interval != self.current_45m_start)

            if interval_changed:
                # 45м интервал закрылся!
                old_msk = DataManager.to_msk_time(self.current_45m_start)
                new_msk = DataManager.to_msk_time(new_45m_interval)

                self.logger.info(
                    f"45М ИНТЕРВАЛ ЗАКРЫТ: {old_msk.strftime('%H:%M')}-{(old_msk + timedelta(minutes=45)).strftime('%H:%M')} МСК")
                self.logger.info(
                    f"45М ИНТЕРВАЛ ОТКРЫТ: {new_msk.strftime('%H:%M')}-{(new_msk + timedelta(minutes=45)).strftime('%H:%M')} МСК")

                # Фиксируем предыдущую свечу
                if self.current_45m_candle and self.current_45m_candle['close'] != 0.0:
                    self.logger.info(f"45М свеча зафиксирована: {self.current_45m_candle['close']}")

                # Начинаем новую 45м свечу
                self.current_45m_start = new_45m_interval
                self.current_45m_candle = None
                self.klines_45m.append(0.0)

                # Ограничиваем размер
                if len(self.klines_45m) > 250:
                    self.klines_45m = self.klines_45m[-200:]

            # Обновляем текущую 45м свечу
            self.update_45m_candle(close_price, high_price, low_price, volume)

            # Обновляем цену в массиве
            if len(self.klines_45m) > 0:
                self.klines_45m[-1] = close_price

            self.calculate_macd()

        except Exception as e:
            self.logger.error(f"Ошибка 45м WebSocket: {e}")

    def start_websocket(self):
        def on_message(_, message):
            if self.timeframe == "5m":
                self.handle_5m_websocket(message)
            else:
                self.handle_45m_websocket(message)

        def on_error(_, error):
            self.logger.error(f"WebSocket ошибка: {error}")

        def on_close(_, __, ___):
            self.logger.info("WebSocket закрыт")
            if self.running:
                time.sleep(5)
                self.start_websocket()

        def on_open(_):
            self.logger.info("WebSocket подключен")

        socket_url = f"wss://fstream.binance.com/ws/{self.ws_stream}"

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        self.logger.info(f"Подключение к: {socket_url}")
        self.running = True
        self.ws.run_forever()

    @staticmethod
    def calculate_ema(prices, period):
        """Расчет EMA с высокой точностью"""
        prices = np.array(prices, dtype=np.float64)

        if len(prices) < period:
            return np.full_like(prices, np.nan, dtype=np.float64)

        ema = np.full_like(prices, np.nan, dtype=np.float64)
        alpha = np.float64(2.0) / np.float64(period + 1.0)

        # Находим первый валидный индекс
        first_valid = period - 1
        for i in range(len(prices)):
            if not np.isnan(prices[i]):
                if period == 7:  # Signal EMA
                    if prices[i] != 0:
                        first_valid = max(i, period - 1)
                        break
                else:
                    first_valid = max(i, period - 1)
                    break

        if first_valid >= len(prices):
            return ema

        # Первое значение = SMA
        if first_valid + period <= len(prices):
            valid_window = prices[first_valid - period + 1:first_valid + 1]
            if period == 7:
                if not np.any(np.isnan(valid_window)) and not np.any(valid_window == 0):
                    ema[first_valid] = np.mean(valid_window, dtype=np.float64)
                elif first_valid < len(prices):
                    ema[first_valid] = prices[first_valid]
            else:
                if not np.any(np.isnan(valid_window)):
                    ema[first_valid] = np.mean(valid_window, dtype=np.float64)
                elif first_valid < len(prices):
                    ema[first_valid] = prices[first_valid]

        # Остальные значения по формуле EMA
        for i in range(first_valid + 1, len(prices)):
            if period == 7:
                if not np.isnan(prices[i]) and prices[i] != 0 and not np.isnan(ema[i - 1]):
                    ema[i] = alpha * prices[i] + (np.float64(1.0) - alpha) * ema[i - 1]
            else:
                if not np.isnan(prices[i]) and not np.isnan(ema[i - 1]):
                    ema[i] = alpha * prices[i] + (np.float64(1.0) - alpha) * ema[i - 1]

        return ema

    def calculate_macd(self):
        """Расчет MACD индикатора"""
        fast_period = self.config['MACD_FAST']
        slow_period = self.config['MACD_SLOW']
        signal_period = self.config['MACD_SIGNAL']

        min_required = max(slow_period, fast_period) + signal_period - 1

        if self.timeframe == "5m":
            if len(self.klines_data) < min_required:
                return
            prices = np.array(self.klines_data, dtype=np.float64)
        else:
            if len(self.klines_45m) < min_required:
                return
            prices = np.array(self.klines_45m, dtype=np.float64)

        # Рассчитываем EMA
        ema_fast = DataManager.calculate_ema(prices, fast_period)
        ema_slow = DataManager.calculate_ema(prices, slow_period)

        # MACD Line
        macd_line = np.full_like(prices, np.nan, dtype=np.float64)
        for i in range(len(prices)):
            if not np.isnan(ema_fast[i]) and not np.isnan(ema_slow[i]):
                macd_line[i] = ema_fast[i] - ema_slow[i]

        # Signal Line
        first_macd_idx = None
        for i in range(len(macd_line)):
            if not np.isnan(macd_line[i]):
                first_macd_idx = i
                break

        if first_macd_idx is None or len(macd_line) - first_macd_idx < signal_period:
            return

        valid_macd = macd_line[first_macd_idx:]
        signal_ema = DataManager.calculate_ema(valid_macd, signal_period)

        signal_line = np.full_like(prices, np.nan, dtype=np.float64)
        signal_line[first_macd_idx:] = signal_ema

        # Получаем последние значения
        current_macd = float(macd_line[-1]) if not np.isnan(macd_line[-1]) else 0.0
        current_signal = float(signal_line[-1]) if not np.isnan(signal_line[-1]) else 0.0
        current_histogram = current_macd - current_signal

        if np.isnan(current_macd) or np.isnan(current_signal):
            return

        self.macd_data = {
            'macd': current_macd,
            'signal': current_signal,
            'histogram': current_histogram
        }

    def get_current_interval_info(self):
        """Информация о текущем интервале для статуса"""
        with self.lock:
            if self.timeframe == "5m":
                if self.current_interval_start:
                    start_msk = DataManager.to_msk_time(self.current_interval_start)
                    end_msk = start_msk + timedelta(minutes=5)
                    return f"{start_msk.strftime('%H:%M')}-{end_msk.strftime('%H:%M')} МСК (5m)"
                return "N/A (5m)"
            else:
                if self.current_45m_start:
                    start_msk = DataManager.to_msk_time(self.current_45m_start)
                    end_msk = start_msk + timedelta(minutes=45)
                    return f"{start_msk.strftime('%H:%M')}-{end_msk.strftime('%H:%M')} МСК (45m)"
                return "N/A (45m)"

    def get_macd_data(self):
        with self.lock:
            return self.macd_data.copy()

    def get_last_websocket_price(self):
        with self.lock:
            return self.last_websocket_price

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()