import polars as pl
from constants import logging, O_SETG
from datetime import datetime


def make_candles_from_ticks(data):
    try:
        MARKET_OPEN = datetime.strptime("00:00:00", "%H:%M:%S")
        candle = int(O_SETG["trade"]["candle"])

        df = pl.DataFrame(data, schema=["timestamp", "token", "price", "volume_traded"])

        if df is None or df.height == 0:
            return []

        df = df.with_columns(
            (pl.col("volume_traded") - pl.col("volume_traded").shift(1))
            .fill_null(pl.col("volume_traded"))  # Fix first row issue
            .clip(0)
            .alias("volume")
        ).drop("volume_traded")

        # Compute the minute time bins
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns")))
        df = df.with_columns(
            (
                (pl.col("timestamp") - MARKET_OPEN).dt.total_minutes()
                // candle
                * candle
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
    except Exception as e:
        logging.error(f"Error making candles: {e}")
        return []
