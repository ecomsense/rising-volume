from constants import O_SETG, logging
from helper import Helper
from traceback import print_exc
import numpy as np
import pendulum as pdlm


class Exit:
    _targets = O_SETG["trade"]["targets"]
    emit = -1

    def __init__(self, order_id, instrument_token):
        self._order_id = order_id
        self._instrument_token = instrument_token
        self._orders = []
        self._bands = []
        self._orderbook_item = {}
        self.cancel_at = None
        self._fn = "_set_properties"

    """
        common method 
    """

    def _pop_item_from_orderbook(self):
        for buy_order in self._orders:
            if self._order_id == buy_order["order_id"]:
                return buy_order

    def _set_properties(self):
        item = self._pop_item_from_orderbook()
        self._symbol = item["symbol"]
        self._exchange = item["exchange"]
        self._quantity = item["quantity"]
        self._fn = "check_buy_status"

    def check_buy_status(self):
        item = self._pop_item_from_orderbook()
        status = item["status"]
        if status == "COMPLETE":
            self.prepare_and_cover()
        elif status == "CANCELED" or status == "REJECTED":
            logging.info(f"order {self._order_id} is CANCELED or REJECTED")
            self._fn = None
        elif self.cancel_at and pdlm.now("Asia/Kolkata") > self.cancel_at:
            logging.debug(f"attempting to cancel {self._order_id}")
            kwargs = {"order_id": self._order_id}
            Helper.api.order_cancel(**kwargs)

    """
        prepare and cover 
    """

    def _set_sell_params(self):
        # settings
        threshold = O_SETG["trade"]["threshold"]
        self._threshold = threshold * self._fill_price / 100
        self._current_target = 0
        self._stop_price = self._fill_price - self._threshold * 2

    def _set_target(self):
        try:
            # Generate bands (merging initial bands)
            self._bands = np.concatenate(
                (
                    [self._fill_price - (self._fill_price * 10 / 100)],
                    np.linspace(
                        self._fill_price + self._threshold,
                        self._fill_price + self._targets * self._threshold,
                        num=self._targets,
                    ),
                )
            )
            if not isinstance(self._bands, np.ndarray) or self._bands.ndim != 1:
                raise ValueError("self._bands must be a 1D numpy array.")
            if not np.isscalar(self._ltp):
                raise ValueError("self._ltp must be a scalar value.")

            self._bands = [round(b * 20) / 20 for b in self._bands]

            self._stop_price = self._bands[0]
        except Exception as e:
            print_exc()
            logging.error(f"{e} while set target")

    def _place_initial_stop(self):
        try:
            """
            price=0,
            trigger_price=self._stop_price,
            order_type="SL-M",
            """
            sargs = dict(
                symbol=self._symbol,
                quantity=self._quantity,
                product="MIS",
                side="SELL",
                price=self._stop_price - 10,
                trigger_price=self._stop_price,
                order_type="SL",
                exchange=self._exchange,
            )
            logging.debug(sargs)
            self._order_id = Helper.place_order(sargs)
            if self._order_id is None:
                raise RuntimeError(
                    "unable to get order number for initial stop. please manage"
                )
            else:
                self._fn = "look_to_trail"
        except Exception as e:
            logging.error(f"{e} whle place sell order")
            print_exc()

    def prepare_and_cover(self):
        try:
            self._fill_price = Helper.find_fillprice_from_order_id(self._order_id)
            self._set_sell_params()
            self._set_target()
            self._place_initial_stop()
        except Exception as e:
            print(f"{e} while prepare and cover")

    """
        look_to_trail 
    """

    def _is_exit_conditions(self):
        try:
            Flag = False
            if self._ltp < self._stop_price:
                logging.info(
                    f"Trail stopped {self._ltp} is less than {self._stop_price}"
                )
                Flag = True
            elif self._ltp > self._bands[-1]:
                # LTP moved to a highest band
                logging.info(f"LTP {self._ltp} reached final target{self._bands[-1]}")
                Flag = True
        except Exception as e:
            logging.error(f"{e} while check stop loss")
            print_exc()
        finally:
            return Flag

    def _cover_to_close(self):
        try:
            args = dict(
                variety="regular",
                order_id=self._order_id,
                quantity=self._quantity,
                order_type="MARKET",
                trigger_price=0.0,
                price=0.00,
            )
            logging.debug(f"modify order {args}")
            resp = Helper.modify_order(args)
            logging.debug(f"order id: {args['order_id']} {resp}")
        except Exception as e:
            logging.error(f"{e} while exit order")
            print_exc()

    def _update_targets(self):
        try:
            # Find the new target index for the current LTP
            new_target = np.searchsorted(self._bands, self._ltp, side="right") - 1
            if new_target > self._current_target:
                if self._current_target == 0:
                    self._stop_price = self._fill_price
                else:
                    self._stop_price = self._bands[self._current_target]

                self._current_target = new_target
                logging.info(
                    f"LTP {self._ltp}: is above Target {new_target}. New stop trailing: {self._stop_price}"
                )
            elif new_target < self._current_target:
                # LTP moved to a lower band (optional warning)
                logging.debug(
                    f"LTP {self._ltp}: Dropped below Target {new_target}. Current stop remains: {self._stop_price}"
                )
            else:  # no change
                logging.debug(f"LTP {self._ltp}: Stop remains unchanged")
        except Exception as e:
            print_exc()
            logging.error(f"{e} in update target")

    def look_to_trail(self):
        try:

            logging.debug(f"{self._bands}")
            item = self._pop_item_from_orderbook()
            status = item["status"]
            if status == "COMPLETE":
                logging.info("initial stop loss hit")
                self.emit = int(O_SETG["trade"]["candle"])
                self._fn = None
                return
            elif status == "CANCELED" or status == "REJECTED":
                logging.info(f"order {self._order_id} is CANCELED or REJECTED")
                self._fn = None
                return

            if self._is_exit_conditions():
                self.emit = 0
                self._cover_to_close()
                self._fn = None
                return

            self._update_targets()

        except Exception as e:
            logging.error(f"{e} in look_to_trail")
            print_exc()

    def run(self, orders, ltps):
        try:
            self._orders = orders
            self._ltp = ltps[self._instrument_token]
            getattr(self, self._fn)()
        except Exception as e:
            logging.error(f"{e} in run for buy order {self._order_id}")
            print_exc()


if __name__ == "__main__":
    try:
        entry_price = 100
        buy_order = {
            "symbol": "NIFTY",
            "average_price": entry_price,
            "quantity": 10,
            "product": "MIS",
            "exchange": "NSE",
        }

        tsl = Exit(order_id=1, instrument_token=2)

        # Simulate LTP updates
        ltp_values = [94, 96, 99, 103, 108, 112, 116, 123, 135, 145]
        for ltp in ltp_values:
            tsl._set_target()
            tsl._ltp = ltp
            action = tsl.look_to_trail()
            if action == tsl._order_id:
                print("Exiting strategy.")
                break
    except Exception as e:
        print(e)
        print_exc()
