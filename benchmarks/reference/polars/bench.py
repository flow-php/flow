# usage: POLARS_MAX_THREADS=<threads> python bench.py <scan|to_parquet|group_by|sort> <input.csv|input.parquet> <output-dir>
# POLARS_MAX_THREADS is read once when polars is imported, so it has to come from the environment.
# scan: count the rows that pass a predicate touching every column, so every column is decoded (pl.len() alone is
# answered from the Parquet footer).
import os
import sys

import polars as pl

shape, source, output = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(output, exist_ok=True)

frame = pl.scan_parquet(source) if source.endswith(".parquet") else pl.scan_csv(source, try_parse_dates=True)

scan = (
    (pl.col("id") % 2 >= 0)
    & (pl.col("quantity") % 2 >= 0)
    & (pl.col("amount") >= 0)
    & (pl.col("email") != "")
    & (pl.col("city") != "")
    & (pl.col("country") != "")
    & (pl.col("first_name") != "")
    & (pl.col("last_name") != "")
    & pl.col("order_uuid").is_not_null()
    & pl.col("created_at").is_not_null()
    & pl.col("active").is_not_null()
)

match shape:
    case "scan":
        print(frame.filter(scan).select(pl.len()).collect(engine="streaming").item())
    case "to_parquet":
        frame.sink_parquet(f"{output}/to_parquet.parquet")
    case "group_by":
        frame.group_by("country").agg(
            pl.col("amount").sum().alias("amount_sum"),
            pl.col("id").count().alias("id_count"),
            pl.col("amount").mean().alias("amount_avg"),
        ).sink_parquet(f"{output}/group_by.parquet")
    case "sort":
        frame.sort("amount", descending=True).sink_parquet(f"{output}/sort.parquet")
    case _:
        sys.exit(f"unknown shape: {shape}")
