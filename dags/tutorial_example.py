from airflow.sdk import dag, task
from datetime import datetime, timedelta


@dag(
        default_args={
            "depends_on_past":False,
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
        },
        dag_id="hello_world_taskFlow",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,    
)
def run_intro():

    @task()
    def hello():
        import logging
        logger = logging.getLogger("airflow.task")
        logger.info("Hello Airflow")
        

    @task
    def start():
        import logging
        logger = logging.getLogger("airflow.task")
        logger.info("starting to work with airflow")


    @task.bash()
    def sleep():
        return "sleep 5"


    task_1 = hello()
    task_2 = sleep()
    task_3 = start()

    task_1 >> task_2 >> task_3

run_intro()