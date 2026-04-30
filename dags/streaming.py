import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

NOAA_SWPC_APP    = "/opt/airflow/spark/noaa_swpc_to_hdfs.py"
SILVER_NOAA_APP  = "/opt/airflow/spark/silver_noaa.py"
GOLD_BIO_APP     = "/opt/airflow/spark/gold_biologiste.py"
DATAMART_BIO_APP = "/opt/airflow/spark/datamart_biologiste.py"

SPARK_CONF = {
    "spark.driver.host":        os.environ.get("SPARK_LOCAL_IP", "0.0.0.0"),
    "spark.driver.bindAddress": "0.0.0.0",
}
POSTGRES_PKG = "org.postgresql:postgresql:42.7.4"


@dag(
    dag_id="streaming",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "noaa", "biologiste"],
)
def streaming():
    fetch_noaa = SparkSubmitOperator(
        task_id="noaa_swpc_fetch",
        conn_id="spark_default",
        application=NOAA_SWPC_APP,
        name="noaa_swpc_ingest",
        application_args=["--logical-date", "{{ logical_date.isoformat() }}"],
        conf=SPARK_CONF,
        verbose=False,
    )

    silver_noaa = SparkSubmitOperator(
        task_id="silver_noaa",
        conn_id="spark_default",
        application=SILVER_NOAA_APP,
        name="silver_noaa",
        conf=SPARK_CONF,
        verbose=False,
    )

    gold_biologiste = SparkSubmitOperator(
        task_id="gold_biologiste",
        conn_id="spark_default",
        application=GOLD_BIO_APP,
        name="gold_biologiste",
        conf=SPARK_CONF,
        verbose=False,
    )

    datamart_biologiste = SparkSubmitOperator(
        task_id="datamart_biologiste",
        conn_id="spark_default",
        application=DATAMART_BIO_APP,
        name="datamart_biologiste",
        packages=POSTGRES_PKG,
        conf=SPARK_CONF,
        verbose=False,
    )

    fetch_noaa >> silver_noaa >> gold_biologiste >> datamart_biologiste


streaming()
