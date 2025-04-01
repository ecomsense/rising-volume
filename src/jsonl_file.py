import json
from datetime import datetime
from constants import logging, S_DATA, O_FUTL
from toolkit.kokoo import timer
from make_candles import make_candles_from_ticks

MARKET_OPEN = datetime.strptime("00:00:00", "%H:%M:%S")


class JsonlFile:
    def _cleanup(self):
        try:
            if not O_FUTL.is_file_exists(self.JSONL_FILE):
                print("creating data dir")
                O_FUTL.add_path(self.JSONL_FILE)
            else:
                print("deleting ticks file")
                O_FUTL.del_file(self.JSONL_FILE)
                timer(5)
                self._cleanup()
        except Exception as e:
            logging.error(f"e {e} while _cleanup {self}")

    def __init__(self, jsonl_file=None):
        self.JSONL_FILE = S_DATA + "ticks.jsonl" if not jsonl_file else jsonl_file
        self._cleanup()

    def write(self, ticks):
        with open(self.JSONL_FILE, "a") as f:
            for tick in ticks:
                tick["timestamp"] = int(datetime.now().timestamp() * 1e9)  # nanoseconds
                f.write(json.dumps(tick) + "\n")

    def read(self, key):
        """Reads tick data from the file and returns a Polars DataFrame."""
        data = []
        try:
            with open(self.JSONL_FILE, "r") as f:
                for line in f:
                    tick = json.loads(line.strip())
                    if tick["instrument_token"] == key:
                        data.append(
                            [
                                tick["timestamp"],
                                tick["instrument_token"],
                                tick["last_price"],
                                tick.get("volume_traded", 0),
                            ]
                        )
        except Exception as e:
            logging.error(f"Error reading ticks: {e}")
        finally:
            return data

    def candles(self, token):
        try:
            data = self.read(token)
            if data and any(data):
                return make_candles_from_ticks(data)
            else:
                return []
        except Exception as e:
            logging.error(f"Error making candles: {e}")
            return []


if __name__ == "__main__":
    try:
        resp = JsonlFile().candles(13118466)
        print(resp)
    except KeyboardInterrupt:
        __import__("sys").exit()
