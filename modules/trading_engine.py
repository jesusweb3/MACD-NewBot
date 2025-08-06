# modules/trading_engine.py

from pybit.unified_trading import HTTP
import time


class TradingEngine:
    def __init__(self, api_key, secret, symbol, position_size, leverage, logger):
        self.api_key = api_key
        self.secret = secret
        self.symbol = symbol
        self.position_size = position_size
        self.leverage = leverage
        self.logger = logger

        self.session = HTTP(
            testnet=False,
            api_key=self.api_key,
            api_secret=self.secret
        )

        self.current_position = None

        self.qty_step = None
        self.min_order_qty = None
        self.max_order_qty = None
        self.tick_size = None

        self.get_instrument_info()
        self.setup_leverage()

    def get_instrument_info(self):
        """Получение информации об активе для правильного округления размера позиции"""
        try:
            response = self.session.get_instruments_info(
                category="linear",
                symbol=self.symbol
            )

            if response['retCode'] == 0 and response['result']['list']:
                instrument = response['result']['list'][0]

                lot_size_filter = instrument['lotSizeFilter']
                self.qty_step = float(lot_size_filter['qtyStep'])
                self.min_order_qty = float(lot_size_filter['minOrderQty'])
                self.max_order_qty = float(lot_size_filter['maxOrderQty'])

                price_filter = instrument['priceFilter']
                self.tick_size = float(price_filter['tickSize'])

                self.logger.info(
                    f"Параметры {self.symbol}: QtyStep={self.qty_step}, MinQty={self.min_order_qty}, TickSize={self.tick_size}")

            else:
                self.logger.error(
                    f"Не удалось получить информацию об инструменте: {response.get('retMsg', 'Unknown error')}")
                self.qty_step = 0.001
                self.min_order_qty = 0.001
                self.max_order_qty = 1000000
                self.tick_size = 0.01
                self.logger.warning(f"Используются стандартные параметры для {self.symbol}")

        except Exception as e:
            self.logger.api_error("Bybit", f"Ошибка получения информации об инструменте: {e}")
            self.qty_step = 0.001
            self.min_order_qty = 0.001
            self.max_order_qty = 1000000
            self.tick_size = 0.01
            self.logger.warning(f"Используются стандартные параметры для {self.symbol}")

    def round_quantity(self, quantity):
        """Количесвто quantity в соответствии с биржей"""
        if self.qty_step is None:
            return round(quantity, 3)

        precision = len(str(self.qty_step).split('.')[-1]) if '.' in str(self.qty_step) else 0
        rounded_qty = round(quantity / self.qty_step) * self.qty_step
        rounded_qty = round(rounded_qty, precision)

        if rounded_qty < self.min_order_qty:
            rounded_qty = self.min_order_qty
        elif rounded_qty > self.max_order_qty:
            rounded_qty = self.max_order_qty

        return rounded_qty

    def round_price(self, price):
        """Округление цены в соответствии с API"""
        if self.tick_size is None:
            return round(price, 2)

        precision = len(str(self.tick_size).split('.')[-1]) if '.' in str(self.tick_size) else 0
        rounded_price = round(price / self.tick_size) * self.tick_size
        return round(rounded_price, precision)

    def setup_leverage(self):
        try:
            response = self.session.set_leverage(
                category="linear",
                symbol=self.symbol,
                buyLeverage=str(self.leverage),
                sellLeverage=str(self.leverage)
            )

            if response['retCode'] == 0:
                self.logger.info(f"Плечо установлено {self.leverage}x для {self.symbol}")
            else:
                if response.get('retCode') == 110043:
                    self.logger.info(f"Плечо уже установлено {self.leverage}x для {self.symbol}")
                else:
                    self.logger.error(f"Не удалось установить плечо: {response['retMsg']}")

        except Exception as e:
            if "110043" in str(e):
                self.logger.info(f"Плечо уже установлено {self.leverage}x для {self.symbol}")
            else:
                error_msg = str(e).replace('→', '->')
                self.logger.api_error("Bybit", f"Ошибка установки плеча: {error_msg}")

    def get_account_balance(self):
        try:
            response = self.session.get_wallet_balance(
                accountType="UNIFIED"
            )

            if response['retCode'] == 0:
                for coin in response['result']['list'][0]['coin']:
                    if coin['coin'] == 'USDT':
                        return float(coin['walletBalance'])
            return 0

        except Exception as e:
            self.logger.api_error("Bybit", f"Get balance error: {e}")
            return 0

    def get_current_position(self):
        try:
            response = self.session.get_positions(
                category="linear",
                symbol=self.symbol
            )

            if response['retCode'] == 0 and response['result']['list']:
                position = response['result']['list'][0]
                size = float(position['size'])

                if size > 0:
                    return {
                        'side': position['side'],
                        'size': size,
                        'entry_price': float(position['avgPrice']),
                        'unrealized_pnl': float(position['unrealisedPnl'])
                    }
            return None

        except Exception as e:
            self.logger.api_error("Bybit", f"Get position error: {e}")
            return None

    def get_current_price(self):
        try:
            response = self.session.get_tickers(
                category="linear",
                symbol=self.symbol
            )

            if response['retCode'] == 0 and response['result']['list']:
                return float(response['result']['list'][0]['lastPrice'])
            return 0

        except Exception as e:
            self.logger.api_error("Bybit", f"Get price error: {e}")
            return 0

    def calculate_quantity(self, price):
        """Расчёт и правильное округление количества позиции"""
        total_value = self.position_size * self.leverage
        raw_quantity = total_value / price

        rounded_quantity = self.round_quantity(raw_quantity)

        self.logger.info(f"Расчет количества: {total_value} USDT / {price} = {raw_quantity:.6f} -> {rounded_quantity}")

        return rounded_quantity

    def close_position(self):
        position = self.get_current_position()
        if not position:
            return True

        try:
            opposite_side = "Sell" if position['side'] == "Buy" else "Buy"

            rounded_size = self.round_quantity(position['size'])

            response = self.session.place_order(
                category="linear",
                symbol=self.symbol,
                side=opposite_side,
                orderType="Market",
                qty=str(rounded_size),
                reduceOnly=True
            )

            if response['retCode'] == 0:
                self.logger.position_closed(
                    position['side'],
                    self.get_current_price(),
                    position['unrealized_pnl']
                )
                self.current_position = None
                return True
            else:
                self.logger.error(f"Failed to close position: {response['retMsg']}")
                return False

        except Exception as e:
            self.logger.api_error("Bybit", f"Close position error: {e}")
            return False

    def open_position(self, side):
        if not self.close_position():
            return False

        time.sleep(1)

        current_price = self.get_current_price()
        if current_price == 0:
            self.logger.error("Failed to get current price")
            return False

        quantity = self.calculate_quantity(current_price)
        balance = self.get_account_balance()

        if balance < self.position_size:
            self.logger.insufficient_funds(self.position_size, balance)
            return False

        if quantity < self.min_order_qty:
            self.logger.error(f"Количество {quantity} меньше минимального {self.min_order_qty}")
            return False

        try:
            response = self.session.place_order(
                category="linear",
                symbol=self.symbol,
                side=side,
                orderType="Market",
                qty=str(quantity)
            )

            if response['retCode'] == 0:
                self.logger.position_opened(side, self.position_size, current_price)
                self.current_position = {
                    'side': side,
                    'size': quantity,
                    'entry_price': current_price
                }
                return True
            else:
                self.logger.error(f"Failed to open position: {response['retMsg']}")
                return False

        except Exception as e:
            self.logger.api_error("Bybit", f"Open position error: {e}")
            return False

    def open_long(self):
        return self.open_position("Buy")

    def open_short(self):
        return self.open_position("Sell")