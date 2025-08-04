# modules/data_manager.py

import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
import websocket
import json
import threading
import time
from datetime import datetime, timezone


class DataManager:
    def __init__(self, symbol, timeframe, logger):
        self.symbol = symbol
        self.timeframe = timeframe  # "5m" or "45m"
        self.logger = logger
        self.client = Client()
        self.ws = None

        if self.timeframe == "5m":
            self.current_data = []  # Direct 5m candles
            self.historical_interval = Client.KLINE_INTERVAL_5MINUTE
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"
        else:  # 45m
            self.current_15m_data = []  # Historical 15m for completed candles
            self.current_5m_data = []  # Current 5m for real-time updates
            self.current_45m_candle = None
            self.historical_45m = []
            self.historical_interval = Client.KLINE_INTERVAL_15MINUTE
            self.realtime_interval = Client.KLINE_INTERVAL_5MINUTE
            self.ws_stream = f"{self.symbol.lower()}@kline_5m"  # Always use 5m for real-time

        self.macd_data = {}
        self.running = False
        self.lock = threading.Lock()

        # For interval tracking and anti-spam logging
        self.last_interval_start = None
        self.last_data_signature = None

    def get_historical_data(self, limit=200):
        try:
            if self.timeframe == "5m":
                # Direct 5m data
                klines = self.client.get_klines(
                    symbol=self.symbol,
                    interval=self.historical_interval,
                    limit=limit
                )

                df = pd.DataFrame(klines, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ])

                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['open'] = df['open'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                df['volume'] = df['volume'].astype(float)

                self.current_data = df.to_dict('records')
                self.logger.info(f"Загружено {len(self.current_data)} свечей 5m")

            else:  # 45m
                # Get 15m historical data for completed candles
                klines_15m = self.client.get_klines(
                    symbol=self.symbol,
                    interval=self.historical_interval,
                    limit=limit
                )

                df_15m = pd.DataFrame(klines_15m, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ])

                df_15m['timestamp'] = pd.to_datetime(df_15m['timestamp'], unit='ms')
                df_15m['open'] = df_15m['open'].astype(float)
                df_15m['high'] = df_15m['high'].astype(float)
                df_15m['low'] = df_15m['low'].astype(float)
                df_15m['close'] = df_15m['close'].astype(float)
                df_15m['volume'] = df_15m['volume'].astype(float)

                self.current_15m_data = df_15m.to_dict('records')

                # Get recent 5m data to fill current 45m interval gap
                from datetime import timedelta
                current_time = datetime.now(timezone.utc)

                # Calculate how much 5m data we need for current 45m interval
                total_minutes = current_time.hour * 60 + current_time.minute
                interval_number = total_minutes // 45
                interval_start_minutes = interval_number * 45

                # How many 5m candles elapsed in current 45m interval
                elapsed_minutes = total_minutes - interval_start_minutes
                needed_5m_candles = (elapsed_minutes // 5) + 2  # +2 for safety

                klines_5m = self.client.get_klines(
                    symbol=self.symbol,
                    interval=self.realtime_interval,
                    limit=int(needed_5m_candles)
                )

                df_5m = pd.DataFrame(klines_5m, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ])

                df_5m['timestamp'] = pd.to_datetime(df_5m['timestamp'], unit='ms')
                df_5m['open'] = df_5m['open'].astype(float)
                df_5m['high'] = df_5m['high'].astype(float)
                df_5m['low'] = df_5m['low'].astype(float)
                df_5m['close'] = df_5m['close'].astype(float)
                df_5m['volume'] = df_5m['volume'].astype(float)

                self.current_5m_data = df_5m.to_dict('records')

                self.build_45m_timeframe()

                # Log initial state only once
                self.log_initial_45m_state()

                elapsed_minutes = total_minutes - (interval_number * 45)
                completed_5m_candles = elapsed_minutes // 5

                # Convert interval to MSK for logging
                from datetime import timedelta
                interval_start_msk = current_time.replace(hour=interval_start_minutes // 60,
                                                          minute=interval_start_minutes % 60, second=0,
                                                          microsecond=0) + timedelta(hours=3)
                interval_end_msk = interval_start_msk + timedelta(minutes=45)

                self.logger.info(
                    f"Загружено {len(self.current_15m_data)} свечей 15m и {len(self.current_5m_data)} свечей 5m")
                self.logger.info(
                    f"Интервал {interval_start_msk.strftime('%H:%M')}-{interval_end_msk.strftime('%H:%M')} МСК: "
                    f"прошло {elapsed_minutes} мин, запрошено {completed_5m_candles} завершенных + актуальная через WebSocket")

            self.calculate_macd()

        except BinanceAPIException as e:
            self.logger.api_error("Binance", str(e))

    def build_45m_timeframe(self):
        if len(self.current_15m_data) < 3:
            return

        # Build completed 45m candles from 15m data
        grouped_candles = []
        current_group = []
        current_interval_start = None

        for candle in self.current_15m_data:
            timestamp = candle['timestamp']

            # Ensure timezone-aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            # Calculate 45m interval start from UTC midnight
            total_minutes = timestamp.hour * 60 + timestamp.minute
            interval_number = total_minutes // 45
            interval_start_minutes = interval_number * 45

            start_hour = interval_start_minutes // 60
            start_minute = interval_start_minutes % 60

            interval_start = timestamp.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)

            if not current_group:
                current_group = [candle]
                current_interval_start = interval_start
            elif interval_start == current_interval_start and len(current_group) < 3:
                current_group.append(candle)
            else:
                if len(current_group) == 3:
                    grouped_candles.append(DataManager.create_45m_candle(current_group, current_interval_start))
                current_group = [candle]
                current_interval_start = interval_start

        # Handle last complete group (only if it has 3 candles)
        if len(current_group) == 3:
            grouped_candles.append(DataManager.create_45m_candle(current_group, current_interval_start))

        self.historical_45m = grouped_candles

        # Build current 45m candle from 5m data
        self.build_current_45m_from_5m()

    def build_current_45m_from_5m(self):
        if not self.current_5m_data:
            return

        # Find current 45m interval start (UTC)
        current_time = datetime.now(timezone.utc)
        total_minutes = current_time.hour * 60 + current_time.minute
        interval_number = total_minutes // 45
        interval_start_minutes = interval_number * 45

        start_hour = interval_start_minutes // 60
        start_minute = interval_start_minutes % 60

        current_interval_start = current_time.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)

        # Check if interval changed (new interval started)
        if self.last_interval_start is not None and self.last_interval_start != current_interval_start:
            # Convert to MSK for logging
            from datetime import timedelta
            old_start_msk = self.last_interval_start + timedelta(hours=3)
            old_end_msk = self.last_interval_start + timedelta(hours=3, minutes=45)
            new_start_msk = current_interval_start + timedelta(hours=3)
            new_end_msk = current_interval_start + timedelta(hours=3, minutes=45)

            self.logger.info(f"ЗАВЕРШЕН интервал {old_start_msk.strftime('%H:%M')}-{old_end_msk.strftime('%H:%M')} МСК")
            self.logger.info(
                f"НАЧАТ новый интервал {new_start_msk.strftime('%H:%M')}-{new_end_msk.strftime('%H:%M')} МСК")

        self.last_interval_start = current_interval_start

        # Collect ALL 5m candles for current 45m interval
        current_interval_5m = []
        completed_candles = 0
        current_candles = 0

        for candle in self.current_5m_data:
            candle_time = candle['timestamp']

            # Ensure both times are timezone-aware for comparison
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)

            # Check if this 5m candle belongs to current 45m interval
            if candle_time >= current_interval_start:
                from datetime import timedelta
                interval_end = current_interval_start + timedelta(minutes=45)
                if candle_time < interval_end:
                    current_interval_5m.append(candle)

                    # Determine if this is completed or current candle
                    candle_end = candle_time + timedelta(minutes=5)
                    if current_time >= candle_end:
                        completed_candles += 1
                    else:
                        current_candles += 1

        if current_interval_5m:
            self.current_45m_candle = DataManager.create_45m_candle(current_interval_5m, current_interval_start,
                                                                    is_current=True)

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

    def calculate_macd(self, fast=12, slow=26, signal=7):
        if self.timeframe == "5m":
            if len(self.current_data) < slow:
                return
            closes = [c['close'] for c in self.current_data]
        else:  # 45m
            if len(self.historical_45m) < slow:
                return
            closes = [c['close'] for c in self.historical_45m]
            if self.current_45m_candle:
                closes.append(self.current_45m_candle['close'])

        closes = np.array(closes)

        # Calculate EMAs
        ema_fast = DataManager._calculate_ema(closes, fast)
        ema_slow = DataManager._calculate_ema(closes, slow)

        # MACD line
        macd_line = ema_fast - ema_slow

        # Signal line
        signal_line = DataManager._calculate_ema(macd_line, signal)

        self.macd_data = {
            'macd': float(macd_line[-1]) if len(macd_line) > 0 else 0,
            'signal': float(signal_line[-1]) if len(signal_line) > 0 else 0,
            'histogram': float(macd_line[-1] - signal_line[-1]) if len(macd_line) > 0 and len(signal_line) > 0 else 0
        }

    @staticmethod
    def _calculate_ema(data, period):
        alpha = 2 / (period + 1)
        ema = np.zeros_like(data)
        ema[0] = data[0]

        for i in range(1, len(data)):
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]

        return ema

    def start_websocket(self):
        def on_message(_, message):
            try:
                data = json.loads(message)
                if 'k' in data:
                    kline = data['k']

                    new_candle = {
                        'timestamp': pd.to_datetime(kline['t'], unit='ms'),
                        'open': float(kline['o']),
                        'high': float(kline['h']),
                        'low': float(kline['l']),
                        'close': float(kline['c']),
                        'volume': float(kline['v']),
                        'is_closed': kline['x']
                    }

                    self.update_data(new_candle)

            except Exception as e:
                self.logger.error(f"Ошибка обработки WebSocket сообщения: {e}")

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
        socket_url = f"wss://stream.binance.com:9443/ws/{stream}"

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        self.running = True
        self.ws.run_forever()

    def update_data(self, new_candle):
        with self.lock:
            if self.timeframe == "5m":
                # Direct 5m handling
                if self.current_data and not new_candle['is_closed']:
                    # Update current candle
                    self.current_data[-1] = {
                        'timestamp': new_candle['timestamp'],
                        'open': new_candle['open'],
                        'high': new_candle['high'],
                        'low': new_candle['low'],
                        'close': new_candle['close'],
                        'volume': new_candle['volume']
                    }
                elif new_candle['is_closed']:
                    # Add completed candle
                    self.current_data.append({
                        'timestamp': new_candle['timestamp'],
                        'open': new_candle['open'],
                        'high': new_candle['high'],
                        'low': new_candle['low'],
                        'close': new_candle['close'],
                        'volume': new_candle['volume']
                    })
                self.calculate_macd()
            else:
                # 45m handling from 5m data for real-time updates
                if self.current_5m_data and not new_candle['is_closed']:
                    # Update current 5m candle
                    self.current_5m_data[-1] = {
                        'timestamp': new_candle['timestamp'],
                        'open': new_candle['open'],
                        'high': new_candle['high'],
                        'low': new_candle['low'],
                        'close': new_candle['close'],
                        'volume': new_candle['volume']
                    }
                elif new_candle['is_closed']:
                    # Add completed 5m candle
                    self.current_5m_data.append({
                        'timestamp': new_candle['timestamp'],
                        'open': new_candle['open'],
                        'high': new_candle['high'],
                        'low': new_candle['low'],
                        'close': new_candle['close'],
                        'volume': new_candle['volume']
                    })

                # Rebuild current 45m candle from 5m data
                self.build_current_45m_from_5m()
                self.calculate_macd()

    def log_initial_45m_state(self):
        """Log 45m candle state only during initialization"""
        if not self.current_45m_candle:
            return

        # Find current 45m interval
        current_time = datetime.now(timezone.utc)
        total_minutes = current_time.hour * 60 + current_time.minute
        interval_number = total_minutes // 45
        interval_start_minutes = interval_number * 45

        start_hour = interval_start_minutes // 60
        start_minute = interval_start_minutes % 60

        current_interval_start = current_time.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)

        # Count completed vs current candles
        current_interval_5m = []
        completed_candles = 0
        current_candles = 0

        for candle in self.current_5m_data:
            candle_time = candle['timestamp']

            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)

            if candle_time >= current_interval_start:
                from datetime import timedelta
                interval_end = current_interval_start + timedelta(minutes=45)
                if candle_time < interval_end:
                    current_interval_5m.append(candle)

                    candle_end = candle_time + timedelta(minutes=5)
                    if current_time >= candle_end:
                        completed_candles += 1
                    else:
                        current_candles += 1

        # Convert to MSK for logging
        from datetime import timedelta
        start_msk = current_interval_start + timedelta(hours=3)
        end_msk = current_interval_start + timedelta(hours=3, minutes=45)

        self.logger.info(f"45m свеча {start_msk.strftime('%H:%M')}-{end_msk.strftime('%H:%M')} МСК: "
                         f"из {len(current_interval_5m)} свечей 5m ({completed_candles} завершенных + {current_candles} текущих)")

    def get_current_timeframe_status(self):
        with self.lock:
            if self.timeframe == "5m":
                if not self.current_data:
                    return None, False
                # 5m candle is complete when WebSocket sends is_closed=True
                last_candle = self.current_data[-1]
                return last_candle, False  # Always processing real-time
            else:
                # 45m logic - check if 45m interval completed
                if not self.current_45m_candle:
                    return None, False

                current_time = datetime.now(timezone.utc)
                candle_start = self.current_45m_candle['timestamp']
                from datetime import timedelta
                expected_end = candle_start + timedelta(minutes=45)

                is_complete = current_time >= expected_end

                return self.current_45m_candle, is_complete

    def get_macd_data(self):
        with self.lock:
            return self.macd_data.copy()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()