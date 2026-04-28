import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_APP = "/opt/airflow/spark/silver_noaa.py"


@dag(
    dag_id="silver_noaa",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "silver", "spark"],
)
def silver_noaa():
    SparkSubmitOperator(
        task_id="build_silver",
        conn_id="spark_default",
        application=SPARK_APP,
        name="silver_noaa",
        conf={
            "spark.driver.host":        os.environ.get("SPARK_LOCAL_IP", "0.0.0.0"),
            "spark.driver.bindAddress": "0.0.0.0",
        },
        verbose=False,
    )


silver_noaa()
