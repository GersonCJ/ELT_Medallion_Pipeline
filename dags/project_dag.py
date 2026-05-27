from airflow.sdk import dag, task
from datetime import datetime, timedelta


@dag(
    default_args={
        "depends_on_past":False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    dag_id="ELT_DAG",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def runPipelineELT():

    @task
    def extraction():
        from constants import path_strings
        from pathlib import Path
        import logging
        import src.extraction as ext

        # ------------------------------ Path Constants
        url = path_strings.url_main
        url_metadata = path_strings.metadata_url
        bronze_path = Path(path_strings.bronze_path)

        if not any(bronze_path.iterdir()):
            logging.info("Starting extraction ...")
            ext.extract(url, url_metadata)
        else:
            logging.info("Data already available. Skipping extraction ...")


    @task
    def validation():
        from data_quality import gx
        import logging

        gx.run_validation()
        logging.info("Validation concluded ! You can see the results at: http://localhost:8080/gx/uncommitted/data_docs/local_site/")


    @task
    def load():
        from constants import path_strings
        from dotenv import load_dotenv
        from pathlib import Path
        from sqlalchemy import create_engine
        import logging
        import os
        import src.load as ld
        import urllib.parse

        # ------------------------------ Path Constants

        raw_main_path = Path(path_strings.raw_main_path)

        # ------------------------------ Environment variables

        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        raw_parquet = ld.load_silver(raw_main_path)                           # Load .parquet raw dataframe
        encoded_password = urllib.parse.quote_plus(password)                  # Encode Password if it has special characters such as "@"
        engine = create_engine(f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}")
        ld.push_to_db(raw_parquet, "raw_full", engine, schema="staging_raw")  # Push raw directly to Postgres
        logging.info("Data pushed to Postgres DB")


    @task.bash
    def dbt_run():
        return """
            docker exec dbt sh -c "
            dbt run"
        """


    @task.bash
    def dbt_test():
        return """
        docker exec dbt sh -c "
        dbt test"
        """
    

    @task(trigger_rule="all_done")
    def dashboard_creation():
        from gold_metabase import gold
        gold.metabase_pipeline()
    

    @task.bash()
    def docs():
        return """
        docker exec dbt sh -c "
        dbt docs generate" &&
        echo '========================' &&
        echo 'GX docs:          http://localhost:8080/uncommitted/data_docs/local_site/' &&
        echo 'DBT docs:         http://localhost:8181/' &&
        echo 'Metabse:          http://localhost:3000/' &&
        echo '========================' 
        """
    extraction() >> validation() >> load() >> dbt_run() >> dbt_test() >> dashboard_creation() >> docs()


runPipelineELT()