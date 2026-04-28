import argparse
import os
import sys
from urllib.parse import urlparse

from pyspark.sql import SparkSession


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metastore-uri", default=os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
    parser.add_argument("--datamart-dsn",  default=os.environ.get("DATAMART_DSN"))
    parser.add_argument("--target-table",  default="public.ingenieur")
    args = parser.parse_args()

    if not args.datamart_dsn:
        print("DATAMART_DSN env var (or --datamart-dsn) is required", flush=True)
        return 1

    parsed = urlparse(args.datamart_dsn)
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    properties = {
        "user":     parsed.username,
        "password": parsed.password,
        "driver":   "org.postgresql.Driver",
    }

    spark = (
        SparkSession.builder
        .appName("datamart_ingenieur")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", args.metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        df = spark.sql("SELECT * FROM gold.instrument_calibration")
        count = df.count()
        df.write.jdbc(jdbc_url, args.target_table, mode="overwrite", properties=properties)
        print(f"Wrote {count} rows to {jdbc_url} table {args.target_table}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
