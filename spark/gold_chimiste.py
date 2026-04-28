import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FILTER_EMISSION = [
    ("F656N", "Halpha"),
    ("F658N", "Halpha+NII"),
    ("F502N", "OIII"),
    ("F673N", "SII"),
    ("F487N", "Hbeta"),
    ("F631N", "OI"),
    ("F164N", "FeII"),
    ("F343N", "NeV"),
    ("F953N", "SIII"),
    ("F469N", "HeII"),
    ("F505N", "OIII"),
    ("F657N", "Halpha+NII"),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-uri",      default="hdfs://namenode:9000")
    parser.add_argument("--silver-path",   default="/silver/mast/")
    parser.add_argument("--target-path",   default="/gold/narrow_band_targets/")
    parser.add_argument("--metastore-uri", default=os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("gold_chimiste")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", args.metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        silver_path = f"{args.hdfs_uri}{args.silver_path}"
        target      = f"{args.hdfs_uri}{args.target_path}"

        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        if not spark.catalog.tableExists("silver.mast"):
            spark.sql(f"CREATE TABLE silver.mast USING PARQUET LOCATION '{silver_path}'")

        ref = spark.createDataFrame(FILTER_EMISSION, ["filter_code", "emission_line"])

        mast = (
            spark.table("silver.mast")
            .select("target_name", F.explode("filters").alias("filter_code"))
            .filter(F.col("target_name").isNotNull() & (F.col("target_name") != ""))
        )

        gold = (
            mast.join(ref, "filter_code")
            .groupBy("target_name", "filter_code", "emission_line")
            .agg(F.count(F.lit(1)).alias("nb_obs"))
            .withColumnRenamed("filter_code", "filter")
            .select("target_name", "filter", "emission_line", "nb_obs")
        )

        gold.coalesce(1).write.mode("overwrite").parquet(target)

        spark.sql("DROP TABLE IF EXISTS gold.narrow_band_targets")
        spark.sql(f"""
            CREATE TABLE gold.narrow_band_targets
            USING PARQUET
            LOCATION '{target}'
        """)

        count = spark.sql("SELECT COUNT(*) FROM gold.narrow_band_targets").collect()[0][0]
        print(f"Wrote {count} rows to {target}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
