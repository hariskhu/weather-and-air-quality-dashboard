from airflow.sdk import dag, task
from datetime import datetime, timedelta
import sys
import time

sys.path.insert(0, "opt/airflow/app")

@dag(
    dag_id="weather_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(seconds=30),
    catchup=False,
    tags=['test'],
)
def weather_pipeline():
    """Testing Airflow"""

    @task
    def test1():
        print(f"test1 ran at {time.time()}")
        time.sleep(3)

    @task
    def test2():
        print(f"test2 ran at {time.time()}")
        time.sleep(3)

    @task
    def test3():
        print(f"test1 ran at {time.time()}")
        time.sleep(3)

    test1() >> test2() >> test3()

weather_pipeline()