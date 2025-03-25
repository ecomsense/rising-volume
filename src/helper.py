from wserver import Wserver
from toolkit.kokoo import timer
from constants import O_CNFG, logging, O_SETG
from traceback import print_exc
import pendulum as pdlm
from login_get_kite import get_kite
from make_candles import get_ohlc


# add a decorator to check if wait_till is past
def is_not_rate_limited(func):
    # Decorator to enforce a 1-second delay between calls
    def wrapper(*args, **kwargs):
        now = pdlm.now()
        """
        if now < Helper.wait_till:
            timer(1)
        """
        timer(1)
        Helper.wait_till = now.add(seconds=1)
        return func(*args, **kwargs)

    return wrapper


class Helper:
    completed_trades = []
    ws = None
    ltp = None
    _orderbook = []

    @classmethod
    def initialize_api(cls):
        try:
            cls.api = get_kite(**O_CNFG)
            cls.ws = Wserver(cls.api.kite)
            cls.wait_till = pdlm.now().add(seconds=1)
        except Exception as e:
            logging.error(f"api {e}")
            print_exc()

    @classmethod
    @is_not_rate_limited
    def old_history(cls, instrument_token, previous_history):
        try:
            start = O_SETG["trade"]["start"]
            candle = O_SETG["trade"]["candle"]
            lst = start.split(":")
            kwargs = dict(
                instrument_token=instrument_token,
                from_date=pdlm.now()
                .replace(hour=int(lst[0]), minute=int(lst[1]))
                .to_datetime_string(),
                to_date=pdlm.now().to_datetime_string(),
                interval=candle,
            )
            history = cls.api.historical(kwargs)
            if isinstance(history, list):
                previous_history = history
            else:
                raise ValueError(
                    f"history type {type(history)} is not of expected type"
                )
            return previous_history
        except Exception as e:
            print_exc()
            timer(5)
            logging.error(f"{e} while getting old historical")
            cls.old_history(instrument_token, previous_history)

    @classmethod
    def historical(cls, instrument_token, previous_history):
        try:
            history = get_ohlc(instrument_token)
            if any(history):
                previous_history = history
            return previous_history
        except Exception as e:
            print_exc()
            timer(5)
            logging.error(f"{e} while getting new history")

    @classmethod
    def get_quote(cls, instrument_token):
        try:
            while not cls.ws.ltp:
                timer(1)
                print("waiting")
            else:
                return cls.ws.ltp[instrument_token]
        except Exception as e:
            print(f"get_quote {e}")
            print_exc()

    @classmethod
    @is_not_rate_limited
    def trades(cls):
        try:
            lst = []
            lst = cls.api.trades
        except Exception as e:
            logging.error(f"{e} while getting trades")
            print_exc()
        finally:
            return lst

    @classmethod
    @is_not_rate_limited
    def orders(cls):
        try:
            if cls.ws.is_orderbook_dirty:
                trade_keys = [
                    "average_price",
                    "exchange",
                    "exchange_update_timestamp",
                    "instrument_token",
                    "order_id",
                    "order_type",
                    "price",
                    "product",
                    "quantity",
                    "side",
                    "status",
                    "symbol",
                    "tag",
                ]
                lst = []
                lst = cls.api.orders
                if any(lst):
                    cls._orderbook = [
                        {k: dct.get(k, None) for k in trade_keys} for dct in lst
                    ]
                    cls.ws.is_orderbook_dirty = False
        except Exception as e:
            logging.error(f"{e} while getting orders")
            print_exc()
        finally:
            return cls._orderbook

    @classmethod
    def find_fillprice_from_order_id(cls, order_id):
        try:
            lst_of_trades = cls.trades()
            lst_of_average_prices = [
                trade["average_price"]
                for trade in lst_of_trades
                if trade["order_id"] == order_id
            ]
            return sum(lst_of_average_prices) / len(lst_of_average_prices)
        except Exception as e:
            print_exc()
            logging.error(f"{e} while find fill price from order id")

    @classmethod
    @is_not_rate_limited
    def positions(cls):
        lst = cls.api.positions
        return lst

    @classmethod
    def entry_order(cls, symbol, exchange, high):
        try:
            kwargs = dict(
                quantity=O_SETG["trade"]["quantity"],
                product="MIS",
                side="BUY",
                symbol=symbol,
                price=high + 0.60,
                trigger_price=high + 0.10,
                order_type="SL",
                exchange=exchange,
            )
            order_no = Helper.place_order(kwargs)
            if order_no:
                logging.debug(f"{order_no=}")
                return order_no
            else:
                print("Order could not be placed")
        except Exception as e:
            print_exc()
            logging.error(f"{e} while get order nos")

    @classmethod
    def place_order(cls, kwargs):
        try:
            return cls.api.order_place(**kwargs)
        except Exception as e:
            print_exc()
            logging.warning(f"{e} while order place {kwargs}")
            kwargs["order_type"] = "LIMIT"
            return cls.api.order_place(**kwargs)

    @classmethod
    def modify_order(cls, kwargs):
        try:
            return cls.api.order_modify(**kwargs)
        except Exception as e:
            print_exc()
            logging.error(e)

    @classmethod
    def cancel_order(cls, order_id):
        try:
            kwargs = {"order_id": order_id, "variety": "regular"}
            return cls.api.order_cancel(**kwargs)
        except Exception as e:
            print_exc()
            logging.error(f"{e} while cancel order {order_id}")


if __name__ == "__main__":
    import pandas as pd
    from constants import S_DATA

    Helper.initialize_api()

    resp = Helper.trades()
    pd.DataFrame(resp).to_csv(S_DATA + "trades.csv")

    resp = Helper.orders()
    pd.DataFrame(resp).to_csv(S_DATA + "orders.csv")

    m2m = 0
    resp = Helper.positions()
    for item in resp:
        print(item)
        m2m += item["m2m"]
    print(f"{m2m=}")

    resp = Helper.api.profile
    print(resp)
