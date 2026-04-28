"""Spark job: ingest NOAA SWPC measurements into a single deduplicated Parquet on HDFS.

Each NOAA endpoint returns an array of measurements; we flatten one row per
measurement, key on (endpoint, event_time), merge with the existing Parquet
table, dedup keeping the latest ingested_at, and atomically overwrite the
target with a single coalesced part-file.

Submitted by the `noaa_swpc_to_hdfs` Airflow DAG via SparkSubmitOperator.
"""

import argparse
import json
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ENDPOINTS = {
    "kp_index":     "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",
    "xray_flux":    "https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json",
    "solar_wind":   "https://services.swpc.noaa.gov/json/ace/swepam/ace_swepam_1h.json",
    "solar_flares": "https://services.swpc.noaa.gov/json/goes/primary/xray-flares-latest.json",
}

# NOAA exposes its event timestamp under different keys per endpoint.
TIME_FIELD = {
    "kp_index":     "time_tag",
    "xray_flux":    "time_tag",
    "solar_wind":   "time_tag",
    "solar_flares": "begin_time",
}

SCHEMA = StructType([
    StructField("endpoint",    StringType(),    nullable=False),
    StructField("event_time",  TimestampType(), nullable=False),
    StructField("record",      StringType(),    nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
])


def _parse_event_time(value: str) -> datetime | None:
    if not value:
        return None
    # NOAA timestamps are ISO-8601 without timezone, naive UTC.
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def fetch_records(ingested_at: datetime) -> list[tuple]:
    rows = []
    for endpoint, url in ENDPOINTS.items():
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            print(f"Failed to fetch {endpoint}: {exc}", flush=True)
            continue

        time_key = TIME_FIELD[endpoint]
        for entry in payload:
            event_time = _parse_event_time(entry.get(time_key))
            if event_time is None:
                continue
            rows.append((endpoint, event_time, json.dumps(entry), ingested_at))
    return rows


def load_existing(spark: SparkSession, path: str):
    hadoop = spark._jvm.org.apache.hadoop
    hadoop_path = hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(hadoop_path):
        return spark.read.schema(SCHEMA).parquet(path)
    return spark.createDataFrame([], SCHEMA)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--logical-date", required=True)
    parser.add_argument("--hdfs-uri", default="hdfs://namenode:9000")
    parser.add_argument("--target-path", default="/bronze/noaa_swpc/data.parquet")
    args = parser.parse_args()

    ingested_at = datetime.now(timezone.utc)
    new_rows = fetch_records(ingested_at)

    spark = (
        SparkSession.builder
        .appName(f"noaa_swpc_{datetime.fromisoformat(args.logical_date).strftime('%Y%m%dT%H%M%S')}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        target = f"{args.hdfs_uri}{args.target_path}"

        existing = load_existing(spark, target)
        new_df = spark.createDataFrame(new_rows, SCHEMA)

        merged = existing.unionByName(new_df)

        window = Window.partitionBy("endpoint", "event_time").orderBy(F.desc("ingested_at"))
        deduped = (
            merged
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        # Cache + materialize to avoid Spark reading the same path it's about to overwrite.
        deduped.persist()
        total = deduped.count()
        new_count = len(new_rows)

        deduped.coalesce(1).write.mode("overwrite").parquet(target)
        deduped.unpersist()

        print(f"Wrote {total} unique rows to {target} (this run brought {new_count} candidates)", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
