from constants import logging, D_SYMBOL
from helper import Helper
from entry import Entry
from exit import O_SETG, Exit
from symbols import Symbols
from toolkit.kokoo import is_time_past, kill_tmux
from traceback import print_exc
import pendulum as pdlm


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


def enter_and_get_args(symbols):
    try:
        args = []
        while len(args) == 0:
            # get atm symbols
            ltp = Helper.get_quote(symbols.instrument_token)
            logging.info(f"current {ltp} of underlying")
            lst = symbols.build_chain(ltp)
            if isinstance(lst, list) and any(lst):
                result: list = Entry(lst[0], lst[1]).run()
                args = result
            else:
                logging.debug(f"list of symbol to enter {lst}")
        else:
            return args
    except Exception as e:
        logging.error(f"{e} while enter and get args")
        print_exc()


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

            completed_strategies = [obj for obj in exit_strategies if obj._fn is None]
            pending_strategies = [obj for obj in exit_strategies if obj._fn is not None]
            if len(completed_strategies) == 1 and len(pending_strategies) == 1:
                completed = completed_strategies[0]
                pending = pending_strategies[0]
                logging.info(f"pending stratergy is currently {pending._fn}")
                emit = completed.emit
                if emit == 0:
                    logging.info("calling strategy is emitting cancel")
                    if pending._fn == "check_buy_status":
                        ltps = Helper.ws.ltp
                        pending.run(Helper.orders(), ltps)
                        if pending._fn == "check_buy_status":
                            kwargs = {"order_id": pending._order_id}
                            resp = Helper.api.order_cancel(**kwargs)
                            logging.info(f"{pending._order_id} cancel returned {resp}")
                elif emit > 0:
                    logging.info(f"calling strategy is emitting {emit} min")
                    pending.cancel_at = pdlm.now("Asia/Kolkata").add(minutes=emit)

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
            # Process trade entry and get order_no, tsym as tuple
            order_symbols: list = enter_and_get_args(symbols)
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
