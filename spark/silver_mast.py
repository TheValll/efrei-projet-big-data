import argparse
import os
import sys
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-uri",     default="hdfs://namenode:9000")
    parser.add_argument("--target-path",  default="/silver/mast/")
    parser.add_argument("--source-table", default="public.mast")
    args = parser.parse_args()

    dsn = os.environ["NASA_DB_RAW_DSN"]
    parsed = urlparse(dsn)
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    properties = {
        "user":     parsed.username,
        "password": parsed.password,
        "driver":   "org.postgresql.Driver",
    }

    spark = (
        SparkSession.builder
        .appName("silver_mast")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        raw = spark.read.jdbc(jdbc_url, args.source_table, properties=properties)

        def text(col: str):
            return F.when(F.trim(F.col(col)) == "", None).otherwise(F.col(col))

        def num(col: str):
            return text(col).cast("double")

        def mjd_to_ts(col: str):
            mjd = num(col)
            return F.when(
                mjd.isNull(), None
            ).otherwise(
                F.from_unixtime((mjd - F.lit(40587.0)) * F.lit(86400.0)).cast("timestamp")
            )

        silver = raw.select(
            text("obs_id").alias("obs_id"),
            text("obs_collection").alias("obs_collection"),
            text("dataproduct_type").alias("dataproduct_type"),
            num("calib_level").cast("int").alias("calib_level"),
            text("target_name").alias("target_name"),
            text("target_classification").alias("target_classification"),
            num("s_ra").alias("s_ra"),
            num("s_dec").alias("s_dec"),
            mjd_to_ts("t_min").alias("t_start"),
            mjd_to_ts("t_max").alias("t_end"),
            num("t_exptime").alias("t_exptime"),
            mjd_to_ts("t_obs_release").alias("release_date"),
            text("instrument_name").alias("instrument_name"),
            F.split(text("filters"), ";").alias("filters"),
            text("wavelength_region").alias("wavelength_region"),
            num("em_min").alias("em_min"),
            num("em_max").alias("em_max"),
            text("proposal_id").alias("proposal_id"),
            text("proposal_pi").alias("proposal_pi"),
            text("proposal_type").alias("proposal_type"),
        ).filter(F.col("obs_id").isNotNull())

        silver.persist()
        count = silver.count()
        target = f"{args.hdfs_uri}{args.target_path}"
        silver.coalesce(4).write.mode("overwrite").parquet(target)
        silver.unpersist()

        print(f"Wrote {count} rows to {target}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
