"""Silver layer for NOAA solar flare events.

Reads bronze parquet (live + archive endpoints), parses the `record` JSON,
extracts and types the event timestamps and X-ray class, dedupes on
(event_start, xray_class), and writes typed parquet to HDFS.

Archive entries store end_time/max_time as HHMM strings relative to the
begin_time date; live entries store full ISO timestamps. We handle both.
"""

import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-uri",    default="hdfs://namenode:9000")
    parser.add_argument("--source-path", default="/bronze/noaa_swpc/data.parquet")
    parser.add_argument("--target-path", default="/silver/noaa_flares/")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("silver_noaa")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )

    try:
        source = f"{args.hdfs_uri}{args.source_path}"
        target = f"{args.hdfs_uri}{args.target_path}"

        bronze = spark.read.parquet(source)
        flares = bronze.filter(F.col("endpoint").isin("solar_flares", "solar_flares_archive"))

        record_schema = StructType([
            StructField("begin_time", StringType(), True),
            StructField("end_time",   StringType(), True),
            StructField("max_time",   StringType(), True),
            StructField("xray_class", StringType(), True),
        ])
        parsed = flares.withColumn("rec", F.from_json("record", record_schema))

        begin_ts = F.coalesce(F.to_timestamp("rec.begin_time"), F.col("event_time"))

        def to_event_ts(col_name: str, begin_ts_col, begin_date_col):
            raw = F.col(f"rec.{col_name}")
            is_hhmm = (F.length(raw) == 4) & raw.rlike(r"^\d{4}$")
            hhmm_ts = F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format(begin_date_col, "yyyy-MM-dd"),
                    F.concat(
                        F.substring(raw, 1, 2),
                        F.lit(":"),
                        F.substring(raw, 3, 2),
                        F.lit(":00"),
                    ),
                )
            )
            hhmm_adjusted = F.when(
                hhmm_ts < begin_ts_col, hhmm_ts + F.expr("INTERVAL 1 DAY")
            ).otherwise(hhmm_ts)
            iso_ts = F.to_timestamp(raw)
            return F.when(is_hhmm, hhmm_adjusted).otherwise(iso_ts)

        with_begin = parsed.withColumn("event_start", begin_ts) \
                           .withColumn("event_start_date", F.to_date("event_start"))

        silver = with_begin.select(
            F.when(F.col("endpoint") == "solar_flares_archive",
                   F.lit("archive")).otherwise(F.lit("live")).alias("source"),
            F.col("event_start"),
            to_event_ts("end_time", F.col("event_start"), F.col("event_start_date")).alias("event_end"),
            to_event_ts("max_time", F.col("event_start"), F.col("event_start_date")).alias("event_max"),
            F.col("rec.xray_class").alias("xray_class"),
            F.regexp_extract(F.col("rec.xray_class"), r"^([ABCMX])(\d+(?:\.\d+)?)", 1).alias("xray_class_letter"),
            F.regexp_extract(F.col("rec.xray_class"), r"^([ABCMX])(\d+(?:\.\d+)?)", 2).cast("double").alias("xray_class_magnitude"),
            F.col("ingested_at"),
        ).filter(
            F.col("event_start").isNotNull()
            & F.col("xray_class").isNotNull()
            & (F.col("event_start") >= F.lit("1990-01-01").cast("timestamp"))
            & (F.col("event_start") <= F.lit("2030-12-31").cast("timestamp"))
        )

        window = Window.partitionBy("event_start", "xray_class").orderBy(F.desc("ingested_at"))
        deduped = (
            silver
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        deduped.persist()
        count = deduped.count()
        deduped.coalesce(2).write.mode("overwrite").parquet(target)
        deduped.unpersist()
        print(f"Wrote {count} unique flares to {target}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
