from redis import Redis
from constants import logging
import json
import pendulum as pdlm


class RediStore:
    def _cleanup(self):
        try:
            timestamp = pdlm.now().timestamp()
            creation_time = 60 * self.MINUTES
            for key in self.r.scan_iter("*"):
                self.r.zremrangebyscore(key, 0, timestamp - creation_time)
        except Exception as e:
            logging.error(f"{e} while _cleanup {self}")

    def __init__(self, TTL_IN_MINUTES=30):
        self.MINUTES = TTL_IN_MINUTES
        self.r = Redis(host="localhost", port=6379, db=0)
        self._cleanup()

    def update(self, ticks):
        try:
            for values in ticks:
                key = values["instrument_token"]
                timestamp = values.get("timestamp", pdlm.now().timestamp())
                self.r.zadd(key, {json.dumps(values): timestamp})
        except Exception as e:
            logging.error(f"e {e} while update {self}")

    def read(self, key):
        try:
            data = []
            for full_ticks in self.r.zrange(key, 0, -1):  # Retrieves all values
                tick = json.loads(full_ticks)
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
            logging.error(f"{e} while read {self}")
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
    qs = RediStore()

    def update_fake_tick(qs):
        i = 0
        for i in range(200):
            tick = dict(token=1234, timestamp=pdlm.now().timestamp(), last_price=20 + i)
            i += 1
            qs.update(tick)

    update_fake_tick(qs)
    print(len(qs.candle(1234)))
    update_fake_tick(qs)
    print(len(qs.candle(1111)))
