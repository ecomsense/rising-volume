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
            ltp = Helper.get_quote(symbols.instrument_token)
            logging.info(f"current {ltp} of underlying")
            lst = symbols.build_chain(ltp)

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
