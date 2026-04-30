import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_APP = "/opt/airflow/spark/noaa_archive_to_hdfs.py"


@dag(
    dag_id="noaa_archive_to_hdfs",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "archive", "bronze", "spark", "backfill"],
)
def noaa_archive_to_hdfs():
    SparkSubmitOperator(
        task_id="submit_backfill",
        conn_id="spark_default",
        application=SPARK_APP,
        name="noaa_archive_backfill",
        application_args=[
            "--start-year", "2010",
            "--end-year",   "2016",
        ],
        conf={
            "spark.driver.host":        os.environ.get("SPARK_LOCAL_IP", "0.0.0.0"),
            "spark.driver.bindAddress": "0.0.0.0",
        },
        verbose=False,
    )


noaa_archive_to_hdfs()
