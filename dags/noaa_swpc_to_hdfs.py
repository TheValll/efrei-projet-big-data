"""Submit the NOAA SWPC ingest Spark job every 10 minutes."""

import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_APP = "/opt/airflow/spark/noaa_swpc_to_hdfs.py"


@dag(
    dag_id="noaa_swpc_to_hdfs",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "hdfs", "bronze", "spark"],
)
def noaa_swpc_to_hdfs():
    SparkSubmitOperator(
        task_id="submit_ingest",
        conn_id="spark_default",
        application=SPARK_APP,
        name="noaa_swpc_ingest",
        application_args=[
            "--logical-date", "{{ logical_date.isoformat() }}",
        ],
        conf={
            "spark.driver.host": os.environ.get("SPARK_LOCAL_IP", "0.0.0.0"),
            "spark.driver.bindAddress": "0.0.0.0",
        },
        verbose=False,
    )


noaa_swpc_to_hdfs()
