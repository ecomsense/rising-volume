from constants import logging, D_SYMBOL
from helper import Helper
from entry import Entry
from exit import O_SETG, Exit
from symbols import Symbols
from toolkit.kokoo import is_time_past, kill_tmux
from traceback import print_exc


def initialize():
    try:
        symbols = Symbols(**D_SYMBOL)
        # initialize api, websocket
        Helper.initialize_api()
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
                ltps = Helper.ws.ltp
                obj.run(Helper.orders(), ltps)
                logging.info(f"next going to {obj._fn} for {obj._order_id}")

            # Filter out completed strategies
            exit_strategies = [obj for obj in exit_strategies if obj._fn is not None]
    except KeyboardInterrupt:
        __import__("sys").exit()
    except Exception as e:
        print_exc()
        logging.error(f"{e} in manage trades")


def main():
    try:
        logging.info("HAPPY TRADING")
        symbols = initialize()
        stop = O_SETG["program"]["stop"]
        while not is_time_past(stop):
            # get atm symbols
            lst = symbols.build_chain(Helper.get_quote(symbols.instrument_token))

            # Process trade entry and get order_no, tsym as tuple
            order_symbols: list = enter_and_get_args(lst)

            manage_trades(order_symbols, symbols)
        else:
            kill_tmux()
        # TODO
    except KeyboardInterrupt:
        __import__("sys").exit()
    except Exception as e:
        print_exc()
        logging.error(f"{e} in  main")


if __name__ == "__main__":
    main()


"""
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
