import argparse
import os
import sys

from pyspark.sql import SparkSession


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-uri",         default="hdfs://namenode:9000")
    parser.add_argument("--silver-mast-path", default="/silver/mast/")
    parser.add_argument("--silver-noaa-path", default="/silver/noaa_flares/")
    parser.add_argument("--target-path",      default="/gold/contaminated_obs/")
    parser.add_argument("--metastore-uri",    default=os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("gold_biologiste")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", args.metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        mast_path = f"{args.hdfs_uri}{args.silver_mast_path}"
        noaa_path = f"{args.hdfs_uri}{args.silver_noaa_path}"
        target    = f"{args.hdfs_uri}{args.target_path}"

        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        if not spark.catalog.tableExists("silver.mast"):
            spark.sql(f"CREATE TABLE silver.mast USING PARQUET LOCATION '{mast_path}'")
        if not spark.catalog.tableExists("silver.noaa_flares"):
            spark.sql(f"CREATE TABLE silver.noaa_flares USING PARQUET LOCATION '{noaa_path}'")

        gold = spark.sql("""
            WITH joined AS (
                SELECT
                    m.obs_id,
                    m.t_start,
                    m.t_end,
                    n.xray_class,
                    CAST(
                        (CASE n.xray_class_letter
                            WHEN 'A' THEN 0
                            WHEN 'B' THEN 20
                            WHEN 'C' THEN 40
                            WHEN 'M' THEN 60
                            WHEN 'X' THEN 80
                            ELSE 0
                        END) + COALESCE(n.xray_class_magnitude, 0.0)
                        AS INT
                    ) AS risk
                FROM silver.mast m
                JOIN silver.noaa_flares n
                  ON n.event_start                                          <= m.t_end
                 AND COALESCE(n.event_end, n.event_start + INTERVAL 1 HOUR) >= m.t_start
                WHERE m.obs_id  IS NOT NULL
                  AND m.t_start IS NOT NULL
                  AND m.t_end   IS NOT NULL
            ),
            ranked AS (
                SELECT
                    obs_id, t_start, t_end, xray_class, risk,
                    ROW_NUMBER() OVER (PARTITION BY obs_id ORDER BY risk DESC) AS rn
                FROM joined
            )
            SELECT
                obs_id,
                t_start,
                t_end,
                xray_class AS flare_class,
                risk       AS risk_score
            FROM ranked
            WHERE rn = 1
        """)

        gold.coalesce(1).write.mode("overwrite").parquet(target)

        spark.sql("DROP TABLE IF EXISTS gold.contaminated_obs")
        spark.sql(f"""
            CREATE TABLE gold.contaminated_obs
            USING PARQUET
            LOCATION '{target}'
        """)

        count = spark.sql("SELECT COUNT(*) FROM gold.contaminated_obs").collect()[0][0]
        print(f"Wrote {count} contaminated observations to {target}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
