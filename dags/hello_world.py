from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_world",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo"],
)
def hello_world():
    @task
    def say_hello():
        print("Hello World!")
        return "Hello World!"

    say_hello()


hello_world()
