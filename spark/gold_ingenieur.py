import argparse
import os
import sys

from pyspark.sql import SparkSession

CALIB_TARGETS = (
    "BIAS",
    "DARK",
    "BLANK",
    "FLAT",
    "FLATFIELD",
    "INTFLAT",
    "CCDFLAT",
    "EARTH-CALIB",
    "EARTHFLAT",
    "TUNGSTEN",
    "DEUTERIUM",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-uri",      default="hdfs://namenode:9000")
    parser.add_argument("--silver-path",   default="/silver/mast/")
    parser.add_argument("--target-path",   default="/gold/instrument_calibration/")
    parser.add_argument("--metastore-uri", default=os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("gold_ingenieur")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", args.metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        silver_path = f"{args.hdfs_uri}{args.silver_path}"
        target      = f"{args.hdfs_uri}{args.target_path}"
        targets_sql = ", ".join(f"'{t}'" for t in CALIB_TARGETS)

        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        if not spark.catalog.tableExists("silver.mast"):
            spark.sql(f"CREATE TABLE silver.mast USING PARQUET LOCATION '{silver_path}'")

        gold = spark.sql(f"""
            WITH monthly AS (
                SELECT
                    instrument_name              AS instrument,
                    DATE_TRUNC('month', t_start) AS year_month,
                    AVG(t_exptime)               AS avg_calib_time
                FROM silver.mast
                WHERE UPPER(target_name) IN ({targets_sql})
                  AND t_start IS NOT NULL
                  AND t_exptime IS NOT NULL
                  AND t_exptime > 0
                GROUP BY instrument_name, DATE_TRUNC('month', t_start)
            )
            SELECT
                instrument,
                CAST(year_month AS DATE) AS year_month,
                avg_calib_time,
                avg_calib_time - LAG(avg_calib_time, 12) OVER (
                    PARTITION BY instrument ORDER BY year_month
                ) AS trend_12m
            FROM monthly
        """)

        gold.coalesce(1).write.mode("overwrite").parquet(target)

        spark.sql("DROP TABLE IF EXISTS gold.instrument_calibration")
        spark.sql(f"""
            CREATE TABLE gold.instrument_calibration
            USING PARQUET
            LOCATION '{target}'
        """)

        count = spark.sql("SELECT COUNT(*) FROM gold.instrument_calibration").collect()[0][0]
        print(f"Wrote {count} rows to {target}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
