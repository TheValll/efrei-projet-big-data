import argparse
import os
import sys

from pyspark.sql import SparkSession


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-uri",      default="hdfs://namenode:9000")
    parser.add_argument("--silver-path",   default="/silver/mast/")
    parser.add_argument("--target-path",   default="/gold/sky_coverage/")
    parser.add_argument("--metastore-uri", default=os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("gold_physicien")
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

        gold = spark.sql("""
            SELECT
                CAST(FLOOR(s_ra)  AS DOUBLE)  AS ra_bin,
                CAST(FLOOR(s_dec) AS DOUBLE)  AS dec_bin,
                COUNT(*)                      AS nb_obs,
                COALESCE(SUM(t_exptime), 0.0) AS total_exptime
            FROM silver.mast
            WHERE s_ra IS NOT NULL AND s_dec IS NOT NULL
            GROUP BY FLOOR(s_ra), FLOOR(s_dec)
        """)

        gold.coalesce(1).write.mode("overwrite").parquet(target)

        spark.sql("DROP TABLE IF EXISTS gold.sky_coverage")
        spark.sql(f"""
            CREATE TABLE gold.sky_coverage
            USING PARQUET
            LOCATION '{target}'
        """)

        count = spark.sql("SELECT COUNT(*) FROM gold.sky_coverage").collect()[0][0]
        print(f"Wrote {count} sky bins to {target}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
