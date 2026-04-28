import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

GOLD_APP     = "/opt/airflow/spark/gold_biologiste.py"
DATAMART_APP = "/opt/airflow/spark/datamart_biologiste.py"


@dag(
    dag_id="datamart_biologiste",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "datamart", "biologiste", "spark"],
)
def datamart_biologiste():
    spark_conf = {
        "spark.driver.host":        os.environ.get("SPARK_LOCAL_IP", "0.0.0.0"),
        "spark.driver.bindAddress": "0.0.0.0",
    }

    build_gold = SparkSubmitOperator(
        task_id="build_gold",
        conn_id="spark_default",
        application=GOLD_APP,
        name="gold_biologiste",
        conf=spark_conf,
        verbose=False,
    )

    materialize = SparkSubmitOperator(
        task_id="materialize_to_postgres",
        conn_id="spark_default",
        application=DATAMART_APP,
        name="datamart_biologiste",
        packages="org.postgresql:postgresql:42.7.4",
        conf=spark_conf,
        verbose=False,
    )

    build_gold >> materialize


datamart_biologiste()
