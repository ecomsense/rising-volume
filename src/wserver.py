from constants import D_SYMBOL, logging, O_CNFG
from kiteconnect import KiteTicker

from jsonl_file import JsonlFile
from redi_store import RediStore


class Wserver:
    is_orderbook_dirty = False

    def __init__(self, kite):
        self.ltp = {}
        self.tokens = []

        if O_CNFG["api_type"] == "bypass":
            self.kws = kite.kws
            self.store = JsonlFile()
        else:
            self.kws = KiteTicker(api_key=kite.api_key, access_token=kite.access_token)
            self.store = RediStore()

        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect
        self.kws.on_noreconnect = self.on_noreconnect
        self.kws.on_order_update = self.on_order_update

        # Infinite loop on the main thread. Nothing after this will run.
        # You have to use the pre-defined callbacks to manage subscriptions.
        self.kws.connect(threaded=True)

    def on_ticks(self, ws, ticks):
        print(self.tokens)
        if any(self.tokens):
            print(f"found tokens {self.tokens}")
            ws.subscribe(self.tokens)
            # Set RELIANCE to tick in `full` mode.
            ws.set_mode(ws.MODE_QUOTE, self.tokens)
            self.tokens = []

        self.ltp.update({dct["instrument_token"]: dct["last_price"] for dct in ticks})
        self.store.write(ticks)

    def on_order_update(self, ws, data):
        self.is_orderbook_dirty = True
        logging.debug("order update : {}".format(data))

    def on_connect(self, ws, response):
        # self.tokens = [v s k, v in nse_symbols.items() if k == "instrument_token"]
        tokens = [D_SYMBOL["instrument_token"]]
        ws.subscribe(tokens)
        # Set RELIANCE to tick in `full` mode.
        ws.set_mode(ws.MODE_QUOTE, tokens)
        logging.debug(f"on connect: {response}")

    def on_close(self, ws, code, reason):
        # On connection close stop the main loop
        # Reconnection will not happen after executing `ws.stop()`
        logging.warning("ws closed with code {}: {}".format(code, reason))
        # ws.stop()

    def on_error(self, ws, code, reason):
        # Callback when connection closed with error.
        logging.error(
            "Connection error: {code} - {reason}".format(code=code, reason=reason)
        )

    def on_reconnect(self, ws, attempts_count):
        # Callback when reconnect is on progress
        logging.warning("Reconnecting: {}".format(attempts_count))

    # Callback when all reconnect failed (exhausted max retries)
    def on_noreconnect(self, ws):
        logging.error("Reconnect failed.")


if __name__ == "__main__":
    from main import initialize
    from helper import Helper
    from toolkit.kokoo import timer

    _ = initialize()

    while True:
        print(Helper.ws.ltp)
        timer(1)
