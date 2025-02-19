import polars as pl


def ohlc(df):
    # Convert time to datetime and create 5-minute bins
    df = df.with_columns(pl.col("timestamp").dt.truncate("5m").alias("5min_bin"))

    # Aggregate OHLC and Volume
    ohlc_df = df.group_by("5min_bin").agg(
        [
            pl.col("last_price").first().alias("open"),
            pl.col("last_price").max().alias("high"),
            pl.col("last_price").min().alias("low"),
            pl.col("last_price").last().alias("close"),
            (pl.col("volume_traded").last() - pl.col("volume_traded").first()).alias(
                "volume"
            ),
        ]
    )
    return ohlc_df


if __name__ == "__main__":
    # Sample tick data (list of dictionaries)
    tick_data = [
        {
            "tradable": True,
            "mode": "quote",
            "instrument_token": 779521,
            "last_price": 737.8,
            "last_traded_quantity": 20,
            "average_traded_price": 740.79,
            "volume_traded": 34054392,
            "total_buy_quantity": 1110725,
            "total_sell_quantity": 1877815,
            "ohlc": {"open": 759.8, "high": 759.9, "low": 731.75, "close": 752.25},
            "change": -1.9209,
            "timestamp": "2024-02-07 09:15:01",
        },
        {
            "tradable": True,
            "mode": "quote",
            "instrument_token": 779521,
            "last_price": 738.5,
            "last_traded_quantity": 25,
            "average_traded_price": 740.78,
            "volume_traded": 34054450,
            "total_buy_quantity": 1111000,
            "total_sell_quantity": 1878000,
            "ohlc": {"open": 759.8, "high": 759.9, "low": 731.75, "close": 752.25},
            "change": -1.8209,
            "timestamp": "2024-02-07 09:15:25",
        },
        {
            "tradable": True,
            "mode": "quote",
            "instrument_token": 779521,
            "last_price": 739.2,
            "last_traded_quantity": 30,
            "average_traded_price": 740.77,
            "volume_traded": 34054510,
            "total_buy_quantity": 1111500,
            "total_sell_quantity": 1878500,
            "ohlc": {"open": 759.8, "high": 759.9, "low": 731.75, "close": 752.25},
            "change": -1.7209,
            "timestamp": "2024-02-07 09:16:10",
        },
    ]

    # Convert tick data into Polars DataFrame
    df = pl.DataFrame(tick_data).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    )

    # Generate 5-minute OHLC
    df = ohlc(df)
    print(df)
