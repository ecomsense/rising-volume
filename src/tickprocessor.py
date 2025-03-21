import polars as pl
from datetime import datetime
from collections import defaultdict
import json

# Constants
MARKET_OPEN = datetime.strptime("00:00:00", "%H:%M:%S")


class TickProcessor:
    def __init__(self, candle_period=5):
        # In-memory storage for ticks, organized by token
        self.tick_buffer = defaultdict(list)
        # Cache for computed candles
        self.candle_cache = defaultdict(list)
        # Last time candles were computed for each token
        self.last_computation = {}
        # Candle period in minutes
        self.candle_period = candle_period
        # Optional: Add a backup frequency to write to disk
        self.backup_counter = 0

    def add_tick(self, tick):
        """Add a new tick to the in-memory buffer and compute candles if needed."""
        token = tick["instrument_token"]

        # Add tick to buffer
        self.tick_buffer[token].append(
            {
                "timestamp": tick["timestamp"],
                "price": tick["last_price"],
                "volume": tick.get("volume_traded", 0),
            }
        )

        # Check if we need to compute candles for this token
        # Only compute after receiving a sufficient number of ticks
        # or if it's been a while since the last computation
        if len(self.tick_buffer[token]) >= 50:  # Adjust threshold as needed
            self._compute_candles_for_token(token)

        # Optional: Periodically write to disk for backup
        self.backup_counter += 1
        if self.backup_counter >= 1000:  # Adjust as needed
            self._backup_ticks()
            self.backup_counter = 0

    def get_ohlc(self, token):
        """Get OHLC candles for a specific token. Compute if not cached recently."""
        # Force computation to ensure we have the latest data
        self._compute_candles_for_token(token)
        return self.candle_cache[token].copy()

    def _compute_candles_for_token(self, token):
        """Compute OHLC candles for a specific token."""
        ticks = self.tick_buffer[token]
        if not ticks:
            return

        # Convert to polars DataFrame for efficient processing
        df = pl.DataFrame(ticks)

        # Compute volume deltas
        df = df.with_columns(
            (pl.col("volume") - pl.col("volume").shift(1))
            .fill_null(pl.col("volume"))
            .clip(0)
            .alias("volume_delta")
        )

        # Compute time bins
        df = df.with_columns(
            (
                (pl.col("timestamp") - int(MARKET_OPEN.timestamp() * 1e9))
                / 1e9
                / 60
                // self.candle_period
                * self.candle_period
            ).alias("time_bin")
        )

        # Aggregate OHLCV per bin
        candles = (
            df.group_by(["time_bin"])
            .agg(
                [
                    pl.col("timestamp").min().alias("from"),
                    pl.col("timestamp").max().alias("to"),
                    pl.col("price").first().alias("open"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").last().alias("close"),
                    pl.col("volume_delta").sum().alias("volume"),
                ]
            )
            .sort("from")
            .to_dicts()
        )

        # Update the cache
        self.candle_cache[token] = candles
        self.last_computation[token] = datetime.now()

        # Truncate buffer to save memory but keep enough data for the next candle
        # Keep only the ticks from the current incomplete candle
        if len(ticks) > 1000:  # Safety threshold
            # Find the timestamp of the last complete candle
            if candles:
                last_complete_candle_time = candles[-1]["to"]
                # Keep only ticks after this time
                self.tick_buffer[token] = [
                    t for t in ticks if t["timestamp"] > last_complete_candle_time
                ]
            else:
                # If no complete candles, just keep the most recent ticks
                self.tick_buffer[token] = ticks[-100:]  # Adjust as needed

    def _backup_ticks(self):
        """Optional: Periodically write ticks to disk as backup."""
        # This can be implemented to periodically save data for recovery
        # but is not essential for the real-time processing
        pass


class Wserver:
    def __init__(self, kite):
        self.ltp = {}
        self.tokens = []
        # Initialize our tick processor with the right candle period from settings
        candle_period = int(O_SETG["trade"]["candle"]) if "O_SETG" in globals() else 5
        self.tick_processor = TickProcessor(candle_period=candle_period)

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

        # Connect
        self.kws.connect(threaded=True)

    def on_ticks(self, ws, ticks):
        if any(self.tokens):
            print(f"found tokens {self.tokens}")
            ws.subscribe(self.tokens)
            ws.set_mode(ws.MODE_QUOTE, self.tokens)
            self.tokens = []

        # Update LTP cache
        self.ltp.update({dct["instrument_token"]: dct["last_price"] for dct in ticks})

        # Process ticks in memory (all in the current thread)
        for tick in ticks:
            self.tick_processor.add_tick(tick)

        # Optionally write to file for backup/persistence
        # Only writing a small percentage of ticks for recovery purposes
        if (
            "write_to_flat_file" in globals()
            and self.tick_processor.backup_counter % 100 == 0
        ):
            write_to_flat_file(ticks)

    # Other methods remain the same...

    def get_ohlc(self, token):
        """Get OHLC data for a specific token."""
        return self.tick_processor.get_ohlc(token)
