import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

NOAA_ARCHIVE_APP = "/opt/airflow/spark/noaa_archive_to_hdfs.py"
SILVER_MAST_APP  = "/opt/airflow/spark/silver_mast.py"
SILVER_NOAA_APP  = "/opt/airflow/spark/silver_noaa.py"

GOLD_APPS = {
    "biologiste": "/opt/airflow/spark/gold_biologiste.py",
    "chimiste":   "/opt/airflow/spark/gold_chimiste.py",
    "ingenieur":  "/opt/airflow/spark/gold_ingenieur.py",
    "physicien":  "/opt/airflow/spark/gold_physicien.py",
}
DATAMART_APPS = {
    "biologiste": "/opt/airflow/spark/datamart_biologiste.py",
    "chimiste":   "/opt/airflow/spark/datamart_chimiste.py",
    "ingenieur":  "/opt/airflow/spark/datamart_ingenieur.py",
    "physicien":  "/opt/airflow/spark/datamart_physicien.py",
}

SPARK_CONF = {
    "spark.driver.host":        os.environ.get("SPARK_LOCAL_IP", "0.0.0.0"),
    "spark.driver.bindAddress": "0.0.0.0",
}
POSTGRES_PKG = "org.postgresql:postgresql:42.7.4"


@dag(
    dag_id="full_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["full", "manual", "backfill"],
)
def full_pipeline():
    noaa_archive = SparkSubmitOperator(
        task_id="noaa_archive",
        conn_id="spark_default",
        application=NOAA_ARCHIVE_APP,
        name="noaa_archive_backfill",
        application_args=["--start-year", "2010", "--end-year", "2016"],
        conf=SPARK_CONF,
        verbose=False,
    )

    silver_mast = SparkSubmitOperator(
        task_id="silver_mast",
        conn_id="spark_default",
        application=SILVER_MAST_APP,
        name="silver_mast",
        packages=POSTGRES_PKG,
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

    noaa_archive >> silver_noaa

    for name, gold_app in GOLD_APPS.items():
        gold = SparkSubmitOperator(
            task_id=f"gold_{name}",
            conn_id="spark_default",
            application=gold_app,
            name=f"gold_{name}",
            conf=SPARK_CONF,
            verbose=False,
        )
        datamart = SparkSubmitOperator(
            task_id=f"datamart_{name}",
            conn_id="spark_default",
            application=DATAMART_APPS[name],
            name=f"datamart_{name}",
            packages=POSTGRES_PKG,
            conf=SPARK_CONF,
            verbose=False,
        )

        if name == "biologiste":
            [silver_mast, silver_noaa] >> gold
        else:
            silver_mast >> gold

        gold >> datamart


full_pipeline()
