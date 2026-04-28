import argparse
import json
import time
from datetime import datetime, timezone

import requests

USER_AGENT = "efrei-bigdata-hubble-lakehouse/1.0 (academic project; contact: valentin-massonniere.ch)"
HTTP_RETRIES = 4
HTTP_BACKOFF_BASE = 5  # seconds
PER_YEAR_DELAY = 2     # seconds, polite spacing between requests
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ARCHIVE_URL = (
    "https://www.ngdc.noaa.gov/stp/space-weather/solar-data/solar-features/"
    "solar-flares/x-rays/goes/xrs/goes-xrs-report_{year}.txt"
)

SCHEMA = StructType([
    StructField("endpoint",    StringType(),    nullable=False),
    StructField("event_time",  TimestampType(), nullable=False),
    StructField("record",      StringType(),    nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
])

CLASS_LETTERS = {"A", "B", "C", "M", "X"}


def parse_line(line: str, default_year: int) -> dict | None:
    # Layout: <5-char prefix><6-char YYMMDD>  HHMM HHMM HHMM [pos 8 chars]  <C> <mag>  GOES <flux> [<AR>]
    # Magnitude is encoded ×10 (e.g. "B 19" -> B1.9, "C 10" -> C1.0, "X 280" -> X28.0).
    if len(line) < 27:
        return None

    head = line[:11]
    if not head.isdigit():
        return None

    yymmdd = head[5:11]
    yy, mm, dd = int(yymmdd[0:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
    year = 2000 + yy if yy < 70 else 1900 + yy
    if year != default_year:
        return None

    rest = line[11:].split()
    if len(rest) < 5:
        return None

    begin_time, end_time, max_time = rest[0], rest[1], rest[2]
    if not (len(begin_time) == 4 and begin_time.isdigit()):
        return None
    bh, bm = int(begin_time[:2]), int(begin_time[2:])

    try:
        event_dt = datetime(year, mm, dd, bh, bm, tzinfo=timezone.utc)
    except ValueError:
        return None

    xray_class = None
    for i in range(3, len(rest) - 1):
        tok = rest[i]
        if len(tok) == 1 and tok in CLASS_LETTERS:
            mag = rest[i + 1]
            if mag.isdigit() and 1 <= len(mag) <= 3:
                xray_class = f"{tok}{int(mag) / 10:.1f}"
                break

    if xray_class is None:
        return None

    return {
        "begin_time":   event_dt.isoformat(),
        "end_time":     end_time if (len(end_time) == 4 and end_time.isdigit()) else None,
        "max_time":     max_time if (len(max_time) == 4 and max_time.isdigit()) else None,
        "xray_class":   xray_class,
        "raw_line":     line.rstrip(),
    }


def fetch_with_retry(url: str) -> str | None:
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(HTTP_RETRIES):
        try:
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code in (429, 503):
                wait = HTTP_BACKOFF_BASE * (2 ** attempt)
                print(f"  HTTP {resp.status_code}, retrying in {wait}s (attempt {attempt + 1}/{HTTP_RETRIES})", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            wait = HTTP_BACKOFF_BASE * (2 ** attempt)
            print(f"  request failed ({exc}), retrying in {wait}s", flush=True)
            time.sleep(wait)
    return None


def fetch_year(year: int) -> list[tuple]:
    url = ARCHIVE_URL.format(year=year)
    print(f"Fetching {url}", flush=True)
    text = fetch_with_retry(url)
    if text is None:
        print(f"  giving up on {year} after {HTTP_RETRIES} attempts", flush=True)
        return []

    ingested_at = datetime.now(timezone.utc)
    rows = []
    skipped = 0

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith(":"):
            continue
        parsed = parse_line(line, year)
        if parsed is None:
            skipped += 1
            continue
        event_time = datetime.fromisoformat(parsed["begin_time"])
        rows.append((
            "solar_flares_archive",
            event_time,
            json.dumps(parsed),
            ingested_at,
        ))

    print(f"  year {year}: parsed={len(rows)} skipped={skipped}", flush=True)
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
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year",   type=int, default=2017)
    parser.add_argument("--hdfs-uri",   default="hdfs://namenode:9000")
    parser.add_argument("--target-path", default="/bronze/noaa_swpc/data.parquet")
    args = parser.parse_args()

    all_rows: list[tuple] = []
    for i, year in enumerate(range(args.start_year, args.end_year + 1)):
        if i > 0:
            time.sleep(PER_YEAR_DELAY)
        all_rows.extend(fetch_year(year))

    if not all_rows:
        print("No archive rows fetched, nothing to write", flush=True)
        return

    spark = (
        SparkSession.builder
        .appName(f"noaa_archive_{args.start_year}_{args.end_year}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        target = f"{args.hdfs_uri}{args.target_path}"
        existing = load_existing(spark, target)
        new_df = spark.createDataFrame(all_rows, SCHEMA)
        merged = existing.unionByName(new_df)

        window = Window.partitionBy("endpoint", "event_time").orderBy(F.desc("ingested_at"))
        deduped = (
            merged
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        deduped.persist()
        total = deduped.count()
        deduped.coalesce(1).write.mode("overwrite").parquet(target)
        deduped.unpersist()

        print(
            f"Wrote {total} unique rows to {target} "
            f"(this backfill brought {len(all_rows)} candidates "
            f"for years {args.start_year}-{args.end_year})",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
