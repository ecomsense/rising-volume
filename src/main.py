from constants import logging, D_SYMBOL
from helper import Helper
from exit import Exit
from symbols import Symbols
from toolkit.kokoo import is_time_past
from traceback import print_exc
from typing import Optional, Tuple  # noqa
from toolkit.kokoo import timer


class Entry:

    def __init__(self, ce, pe):
        self.ce_history, self.pe_history = [], []
        self.ce, self.pe = ce, pe
        self.candle_count = self._get_candle_count()
        self.ce_volume = self._get_volume("ce", -2)
        self.pe_volume = self._get_volume("pe", -2)

    def _get_candle_count(self):
        try:
            COUNT = 0
            # get number of candles
            self.ce_history = Helper.historical(
                self.ce["instrument_token"], self.ce_history
            )
            self.pe_history = Helper.historical(
                self.pe["instrument_token"], self.pe_history
            )
            while len(self.ce_history) != len(self.pe_history):
                timer(1)
                self.ce_history = Helper.historical(self.ce, self.ce_history)
                timer(1)
                self.pe_history = Helper.historical(self.pe, self.pe_history)
            else:
                COUNT = len(self.pe_history)
        except Exception as e:
            print_exc()
            logging.error(f"{e} get candle count")
        finally:
            return COUNT

    def _get_volume(self, ce_or_pe, idx=-1):
        # get volume from history
        call_or_put = getattr(self, ce_or_pe)
        data = Helper.historical(
            call_or_put["instrument_token"], getattr(self, f"{ce_or_pe}_history")
        )
        if len(data) >= abs(idx):
            return data[idx]["volume"]
        else:
            logging.error(f"{data=} length is not greater than requested")
            __import__("sys").exit()

    def _is_volume_increasing(self, ce_or_pe):
        try:
            FLAG = False
            curr_vol = self._get_volume(ce_or_pe)
            prev_vol = getattr(self, f"{ce_or_pe}_volume")
            logging.info(f"{curr_vol=} {prev_vol=} for {ce_or_pe}")
            FLAG = curr_vol > prev_vol
        except Exception as e:
            print_exc()
            logging.warning(f"{e} while is volume increasing")
        finally:
            return FLAG

    def _get_order_numbers(self):
        try:
            order_nos = []
            ce_or_pe = ["ce", "pe"]
            for sym in ce_or_pe:
                tsym = getattr(self, sym)
                symbol = tsym["tradingsymbol"]
                his = getattr(self, f"{sym}_history")
                high = his[-2]["high"]
                order_no = Helper.entry_order(symbol, D_SYMBOL["exchange"], high)
                if order_no:
                    order_nos.append((order_no, symbol))
            return order_nos
        except Exception as e:
            print_exc()
            logging.error(f"{e} while get order numbers")

    def run(self):
        CE_VOLUME_INCREASING = False  # Cache the CE volume state
        while self._get_candle_count() == self.candle_count:
            if not CE_VOLUME_INCREASING:  # Avoid redundant evaluations
                CE_VOLUME_INCREASING = self._is_volume_increasing("ce")

            if CE_VOLUME_INCREASING and self._is_volume_increasing("pe"):
                logging.info("received signal")
                while self._get_candle_count() <= self.candle_count:
                    print(f"waiting for candle {self.candle_count} to complete")
                else:
                    order_nos = self._get_order_numbers()
                    return order_nos
        else:
            return []


def initialize():
    try:
        symbols = Symbols(**D_SYMBOL)
        # initialize api, websocket
        Helper.api()
        # wait till you get quote
        underlying = Helper.get_quote(symbols.instrument_token)
        # build chain from ltp
        chain = symbols.build_chain(underlying, full_chain=True)
        # add option tokens to websocket for subscribing
        Helper.ws.tokens = [dct["instrument_token"] for dct in chain]
        return symbols
    except Exception as e:
        logging.error(f"{e} while initialize objects")
        print_exc()


def enter_and_get_args(lst):
    args = []
    while len(args) == 0:
        args: list = Entry(lst[0], lst[1]).run()
    else:
        return args


def _get_trades(orders):
    try:
        completed_trades = []
        helper_trades: list[dict] = Helper.trades()
        if any(helper_trades):
            # check if the orders are present in tradebook
            completed_trades: list[Optional[dict]] = [
                dct for dct in helper_trades if dct["order_id"] in orders
            ]
            if any(completed_trades):
                # get only the ids of complted trades
                list_of_completed = [dct["order_id"] for dct in completed_trades]
                # find trades that are not completed out of the two
                incomplete = [item for item in orders if item not in list_of_completed]
                if any(incomplete):
                    Helper.cancel_order(incomplete[0])
    except Exception as e:
        print_exc()
        print(f"{e} in get trades")
    finally:
        return completed_trades


"""

def wait_for_trades(orders):
    bought = []
    while len(bought) == 0:
        bought = _get_trades(orders)
    return bought


def manage_exit_strategies(bought_trades, symbols):
    try:
        exit_strategies = []

        # Initialize exit strategies
        for buy_trade in bought_trades:
            logging.debug(f"{buy_trade=}")
            buy_trade["fill_price"] = Helper.find_fillprice_from_order_id(
                buy_trade["order_id"]
            )
            tokens = symbols.tokens_from_symbols(buy_trade["symbol"])
            ltp = Helper.get_quote(tokens[0]["instrument_token"])
            obj_exit = Exit(buy_trade, ltp)
            if obj_exit:
                exit_strategies.append(obj_exit)
        # Process exit strategies
        while any(exit_strategies):
            for obj in exit_strategies:
                logging.debug(f"{obj=}")
                tokens = symbols.tokens_from_symbols(obj._symbol)
                ltp = Helper.get_quote(tokens[0]["instrument_token"])
                obj.run(Helper.orders(), ltp)

            # Filter out completed strategies
            exit_strategies = [obj for obj in exit_strategies if obj._fn is not None]
    except Exception as e:
        print_exc()
        logging.error(f"{e} in manage exit strategies")
"""


def manage_trades(order_symbols: list, symbols: Symbols):
    try:
        exit_strategies = []
        for list_item in order_symbols:
            order, tradingsymbol = list_item
            tokens = symbols.tokens_from_symbols(tradingsymbol)
            obj_exit = Exit(order, tokens[0]["instrument_token"])
            if obj_exit:
                exit_strategies.append(obj_exit)

        while any(exit_strategies):
            for obj in exit_strategies:
                logging.debug(f"{obj=}")
                tokens = symbols.tokens_from_symbols(obj._symbol)
                ltps = Helper.ws.ltp
                obj.run(Helper.orders(), ltps)

            # Filter out completed strategies
            exit_strategies = [obj for obj in exit_strategies if obj._fn is not None]
    except Exception as e:
        print_exc()
        logging.error(f"{e} in manage trades")


def main():
    try:
        logging.info("HAPPY TRADING")
        symbols = initialize()
        while not is_time_past("23:59"):
            # get atm symbols
            lst = symbols.build_chain(Helper.get_quote(symbols.instrument_token))

            # Process trade entry and get order_no, tsym as tuple
            order_symbols: list = enter_and_get_args(lst)

            """
            # Waitsait for trades to complete
            bought_trades = wait_for_trades(orders)

            # Manage exit strategies for completed trades
            manage_exit_strategies(bought_trades, symbols)
            """

            manage_trades(order_symbols, symbols)
        # TODO
    except KeyboardInterrupt:
        __import__("sys").exit()
    except Exception as e:
        print_exc()
        logging.error(f"{e} in  main")


if __name__ == "__main__":
    main()
