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
        self.setup_leverage()

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
                # Error code 110043 means leverage is already set - this is OK
                if response.get('retCode') == 110043:
                    self.logger.info(f"Плечо уже установлено {self.leverage}x для {self.symbol}")
                else:
                    self.logger.error(f"Не удалось установить плечо: {response['retMsg']}")

        except Exception as e:
            # Check if it's the leverage already set error
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
        total_value = self.position_size * self.leverage
        quantity = total_value / price
        return round(quantity, 3)

    def close_position(self):
        position = self.get_current_position()
        if not position:
            return True

        try:
            # Close position by placing opposite order
            opposite_side = "Sell" if position['side'] == "Buy" else "Buy"

            response = self.session.place_order(
                category="linear",
                symbol=self.symbol,
                side=opposite_side,
                orderType="Market",
                qty=str(position['size']),
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
        # Close existing position first
        if not self.close_position():
            return False

        time.sleep(1)  # Wait for position to close

        current_price = self.get_current_price()
        if current_price == 0:
            self.logger.error("Failed to get current price")
            return False

        quantity = self.calculate_quantity(current_price)
        balance = self.get_account_balance()

        if balance < self.position_size:
            self.logger.insufficient_funds(self.position_size, balance)
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