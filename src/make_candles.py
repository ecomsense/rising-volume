import json
import polars as pl
from datetime import datetime
from constants import TICK_FILE, logging, O_SETG

MARKET_OPEN = datetime.strptime("00:00:00", "%H:%M:%S")


def read_ticks():
    """Reads tick data from the file and returns a Polars DataFrame."""
    data = []
    try:
        with open(TICK_FILE, "r") as f:
            for line in f:
                tick = json.loads(line.strip())
                data.append(
                    [
                        tick["timestamp"],
                        tick["instrument_token"],
                        tick["last_price"],
                        tick.get("volume_traded", 0),
                    ]
                )
    except Exception as e:
        logging.error(f" {e} while reading ticks in make candle")
        return None

    return pl.DataFrame(data, schema=["timestamp", "token", "price", "volume_traded"])


def get_ohlc(token):
    """Reads tick data and calculates 5-minute OHLCV on the fly."""
    candle = int(O_SETG["trade"]["candle"])

    df = read_ticks()

    if df is None or df.height == 0:
        return []

    df = df.filter(df["token"] == token).drop("token")

    df = df.with_columns(
        (pl.col("volume_traded") - pl.col("volume_traded").shift(1))
        .fill_null(pl.col("volume_traded"))  # Fix first row issue
        .clip(0)
        .alias("volume")
    ).drop("volume_traded")

    # Compute the 5-minute time bins
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns")))
    df = df.with_columns(
        (
            (pl.col("timestamp") - MARKET_OPEN).dt.total_minutes() // candle * candle
        ).alias("time_bin")
    )

    # Aggregate OHLCV per 5-minute bin
    return (
        df.group_by(["time_bin"])
        .agg(
            [
                pl.col("timestamp").min().alias("from"),
                pl.col("timestamp").max().alias("to"),
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("close"),
                pl.col("volume")
                .sum()
                .alias("volume"),  # Use the corrected per-bin volume
            ]
        )
        .sort("from")
        .to_dicts()
    )


if __name__ == "__main__":
    try:
        token = 13118466
        while True:
            print(get_ohlc(token))
    except KeyboardInterrupt:
        __import__("sys").exit()
