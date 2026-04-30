import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

GOLD_APP     = "/opt/airflow/spark/gold_ingenieur.py"
DATAMART_APP = "/opt/airflow/spark/datamart_ingenieur.py"


@dag(
    dag_id="datamart_ingenieur",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "datamart", "ingenieur", "spark"],
)
def datamart_ingenieur():
    spark_conf = {
        "spark.driver.host":        os.environ.get("SPARK_LOCAL_IP", "0.0.0.0"),
        "spark.driver.bindAddress": "0.0.0.0",
    }

    build_gold = SparkSubmitOperator(
        task_id="build_gold",
        conn_id="spark_default",
        application=GOLD_APP,
        name="gold_ingenieur",
        conf=spark_conf,
        verbose=False,
    )

    materialize = SparkSubmitOperator(
        task_id="materialize_to_postgres",
        conn_id="spark_default",
        application=DATAMART_APP,
        name="datamart_ingenieur",
        packages="org.postgresql:postgresql:42.7.4",
        conf=spark_conf,
        verbose=False,
    )

    build_gold >> materialize


datamart_ingenieur()
