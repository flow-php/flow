# usage: spark-submit --master local[<threads>] bench.py <scan|to_parquet|group_by|sort> <input.csv|input.parquet> <output-dir>
# scan: count the rows that pass a predicate touching every column, so every column is decoded.
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

shape, source, output = sys.argv[1], sys.argv[2], sys.argv[3]

spark = SparkSession.builder.appName(f"flow-reference-{shape}").getOrCreate()
frame = spark.read.parquet(source) if source.endswith(".parquet") else spark.read.csv(source, header=True, inferSchema=True)

scan = (
    (F.col("id") % 2 >= 0)
    & (F.col("quantity") % 2 >= 0)
    & (F.col("amount") >= 0)
    & (F.col("email") != "")
    & (F.col("city") != "")
    & (F.col("country") != "")
    & (F.col("first_name") != "")
    & (F.col("last_name") != "")
    & F.col("order_uuid").isNotNull()
    & F.col("created_at").isNotNull()
    & F.col("active").isNotNull()
)

match shape:
    case "scan":
        print(frame.filter(scan).count())
    case "to_parquet":
        frame.write.mode("overwrite").parquet(f"{output}/to_parquet.parquet")
    case "group_by":
        frame.groupBy("country").agg(
            F.sum("amount").alias("amount_sum"),
            F.count("id").alias("id_count"),
            F.avg("amount").alias("amount_avg"),
        ).write.mode("overwrite").parquet(f"{output}/group_by.parquet")
    case "sort":
        frame.orderBy(F.col("amount").desc()).write.mode("overwrite").parquet(f"{output}/sort.parquet")
    case _:
        sys.exit(f"unknown shape: {shape}")

spark.stop()
