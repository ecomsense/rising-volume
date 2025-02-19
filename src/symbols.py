from constants import O_SETG, S_DATA, O_FUTL, logging
import pandas as pd
from pprint import pprint
from typing import List, Dict, Any
from traceback import print_exc


fpath = S_DATA + "instrument.csv"


def get_symbols(exchange: str) -> Dict[str, Dict[str, Any]]:
    try:
        json = {}
        url = f"https://api.kite.trade/instruments/{exchange}"
        df = pd.read_csv(url)
        # keep only tradingsymbol and instrument_token
        df = df[
            [
                "tradingsymbol",
                "instrument_token",
                "name",
                "strike",
                "instrument_type",
                "expiry",
                "lot_size",
            ]
        ]

        df = df.dropna(axis=1, how="any")
        json = df.to_dict(orient="records")

    except Exception as e:
        print(e)
        print_exc()
    finally:
        return json


def dump():
    if O_FUTL.is_file_not_2day(fpath):
        # Download file & save it.
        url = "https://api.kite.trade/instruments/NFO"
        print("Downloading & Saving Symbol file...")
        df = pd.read_csv(url, on_bad_lines="skip")
        df.fillna(pd.NA, inplace=True)
        df.sort_values(
            ["instrument_type", "exchange"], ascending=[False, False], inplace=True
        )
        df.to_csv(fpath, index=False)


def read():
    df = pd.read_csv(fpath, on_bad_lines="skip")
    df.fillna(pd.NA, inplace=True)
    return df


class Symbols:
    chain = False

    def __init__(self, **kwargs):
        logging.debug("initializing symbols")
        pprint(kwargs)
        if any(kwargs):
            # create property from dictionary
            for key, value in kwargs.items():
                setattr(self, key, value)
                print(f"{key=}: {value=}")
        self.symbols_from_json = O_FUTL.read_file(S_DATA + self.exchange + ".json")

        exchanges = O_SETG["exchanges"]
        for exchange in exchanges:
            exchange_file = S_DATA + exchange + ".json"
            if O_FUTL.is_file_not_2day(exchange_file):
                sym_from_json = get_symbols(exchange)
                O_FUTL.write_file(exchange_file, sym_from_json)

    def tokens_from_symbols(self, symbols: List[str]) -> List:
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            lst = self.chain if self.chain else self.symbols_from_json
            filtered = [dct for dct in lst if dct["tradingsymbol"] in symbols]
            return filtered
        except Exception as e:
            print(f"tokens from symbols error: {e}")
            print_exc()

    def calc_atm_from_ltp(self, ltp) -> int:
        try:
            return round(ltp / self.diff) * self.diff
        except Exception as e:
            print(f"calc atm error: {e}")
            print_exc()

    def build_chain(self, ltp, full_chain=False):
        """
        builds tokens required for the entire chain
        """
        txt = "Build chain" if full_chain else "Straddle"
        atm = self.calc_atm_from_ltp(ltp)
        print(f"{atm=}")
        lst = []
        if not full_chain:
            dist = O_SETG["trade"]["atm_plus"]
            lst.append(self.base + self.expiry + str(atm + (dist * self.diff)) + "PE")
            lst.append(self.base + self.expiry + str(atm - (dist * self.diff)) + "CE")
        else:
            if self.chain:
                return self.chain
            lst.append(self.base + self.expiry + str(atm) + "CE")
            lst.append(self.base + self.expiry + str(atm) + "PE")
            for v in range(1, self.depth):
                txt = self.base + self.expiry + str(atm + (v * self.diff)) + "CE"
                lst.append(txt)
                lst.append(self.base + self.expiry + str(atm + (v * self.diff)) + "PE")
                lst.append(self.base + self.expiry + str(atm - (v * self.diff)) + "CE")
                lst.append(self.base + self.expiry + str(atm - (v * self.diff)) + "PE")
        filtered = self.tokens_from_symbols(lst)
        if full_chain:
            self.chain = filtered
        return filtered

    def get_expiry(self, expiry_offset=0):
        """
        Get the expiry date for the specified base instrument with an optional expiry offset.

        Parameters:
        expiry_offset (int, optional): The offset from the current date to the desired expiry date. Defaults to 0.

        Returns:
        pd.Timestamp or None: The expiry date if found, otherwise None.
        """
        try:
            # Create DataFrame and filter by name
            df = pd.DataFrame(self.symbols_from_json)
            filtered_df = df[df["name"] == self.base]

            # Drop duplicates, convert expiry to datetime, and filter future expiries
            filtered_df = filtered_df.drop_duplicates(subset=["expiry"])
            filtered_df["expiry"] = pd.to_datetime(filtered_df["expiry"])
            today = pd.Timestamp.now().normalize()
            future_expiries = filtered_df[filtered_df["expiry"] >= today]

            # Sort and check offset
            future_expiries = future_expiries.sort_values(by="expiry")
            if 0 <= expiry_offset < len(future_expiries):
                expiry_date = future_expiries.iloc[expiry_offset]["expiry"]
                self.expiry_date = expiry_date
                print(
                    f"Expiry date with offset {expiry_offset} for {self.base}: {expiry_date}"
                )
                return expiry_date
            return None
        except Exception as e:
            print(f"get_straddle error: {e}")
            print_exc()

    def get_option_symbols(self, ltp):
        straddle = self.build_chain(ltp, full_chain=False)
        # Use dictionary comprehension to map instrument types to their symbols
        symbols = {item["instrument_type"]: item["tradingsymbol"] for item in straddle}
        print(f"{symbols=}")

        # Extract the symbols for CE and PE
        ce_symbol = symbols.get("CE")
        pe_symbol = symbols.get("PE")

        logging.debug(f"CE symbol: {ce_symbol}, PE symbol: {pe_symbol}")
        return ce_symbol, pe_symbol


if __name__ == "__main__":
    from constants import D_SYMBOL

    s = Symbols(**D_SYMBOL)
    resp = s.build_chain(24170, full_chain=True)
    print(resp)
    ce_symbol, pe_symbol = s.get_option_symbols(24170)
    print(ce_symbol, pe_symbol)
