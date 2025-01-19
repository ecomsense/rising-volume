from constants import D_SYMBOL, logging, O_CNFG
from kiteconnect import KiteTicker


class Wserver:

    def __init__(self, kite):
        self.ltp = {}
        self.tokens = False

        if O_CNFG["api_type"] == "bypass":
            self.kws = kite.kws
        else:
            self.kws = KiteTicker(api_key=kite.api_key, access_token=kite.access_token)

        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect
        self.kws.on_noreconnect = self.on_noreconnect

        # Infinite loop on the main thread. Nothing after this will run.
        # You have to use the pre-defined callbacks to manage subscriptions.
        self.kws.connect(threaded=True)

    def on_ticks(self, ws, ticks):
        if self.tokens:
            ws.subscribe(self.tokens)
            # Set RELIANCE to tick in `full` mode.
            ws.set_mode(ws.MODE_LTP, self.tokens)
            self.tokens = False
        self.ltp.update({dct["instrument_token"]: dct["last_price"] for dct in ticks})

    def on_connect(self, ws, response):
        # self.tokens = [v for k, v in nse_symbols.items() if k == "instrument_token"]
        tokens = [D_SYMBOL["instrument_token"]]
        ws.subscribe(tokens)
        # Set RELIANCE to tick in `full` mode.
        ws.set_mode(ws.MODE_LTP, tokens)
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
    from constants import S_DATA
    import pickle
    from toolkit.kokoo import timer

    picklepath = f"{S_DATA}/AQD468.pkl"
    with open(picklepath, "rb") as pkl:
        api = pickle.load(pkl)
        ws = Wserver(api.kite)

    while not any(ws.ltp):
        timer(1)
        print("waiting")
    else:
        print(ws.ltp)
