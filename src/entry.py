from helper import Helper
from traceback import print_exc
from constants import logging, D_SYMBOL


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
                self.ce_history = Helper.historical(
                    self.ce["instrument_token"], self.ce_history
                )
                self.pe_history = Helper.historical(
                    self.pe["instrument_token"], self.pe_history
                )
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
        return 0

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
            ce_or_pe = ["pe", "ce"]
            for sym in ce_or_pe:
                tsym = getattr(self, sym)
                symbol = tsym["tradingsymbol"]
                # update history
                _ = self._get_volume(sym)
                his = getattr(self, f"{sym}_history")
                high = his[-2]["high"]
                order_no = Helper.entry_order(symbol, D_SYMBOL["exchange"], high)
                if order_no:
                    order_nos.append((order_no, symbol))
                    txt = f"OHLC of prev candle is {his[-2]} currently candle is {his[-1]}"
                    logging.debug(txt)
            return order_nos
        except Exception as e:
            print_exc()
            logging.error(f"{e} while get order numbers")

    def run(self):
        try:
            CE_VOLUME_INCREASING = False  # Cache the CE volume state
            while self._get_candle_count() == self.candle_count:
                logging.debug(f"candle {self.candle_count} is not yet closed")
                if not CE_VOLUME_INCREASING:  # Avoid redundant evaluations
                    CE_VOLUME_INCREASING = self._is_volume_increasing("ce")

                if CE_VOLUME_INCREASING and self._is_volume_increasing("pe"):
                    logging.info(
                        "received signal and waiting for candle ... to complete"
                    )
                    while self._get_candle_count() <= self.candle_count:
                        print(f"waiting for candle {self.candle_count} to complete")
                    else:
                        order_nos = self._get_order_numbers()
                        return order_nos
            else:
                logging.debug("candle closed. going to check next candle volume")
                return []
        except KeyboardInterrupt:
            __import__("sys").exit()
        except Exception as e:
            logging.error(f"{e} while running entry")
