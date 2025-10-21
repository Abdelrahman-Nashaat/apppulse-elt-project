import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# --- Environment Setup (OS-Agnostic) ---
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_FOLDER = os.path.dirname(DAGS_FOLDER)
PROJECT_ROOT = os.path.dirname(AIRFLOW_FOLDER)

VENV_PATH = os.path.join(PROJECT_ROOT, "venv")
PYTHON_BIN = os.path.join(VENV_PATH, "bin", "python")
DBT_BIN = os.path.join(VENV_PATH, "bin", "dbt")

DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "app_dbt")
SCRIPT_MYSQL = os.path.join(PROJECT_ROOT, "scripts", "ingest_apps_to_mysql.py")
SCRIPT_MONGO = os.path.join(PROJECT_ROOT, "scripts", "ingest_reviews_to_mongodb.py")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='app_pulse_analytics_pipeline',
    default_args=default_args,
    description='Automates the AppPulse ELT pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['apppulse', 'dbt', 'elt'],
) as dag:

    task_ingest_mysql = BashOperator(
        task_id='ingest_apps_to_mysql',
        bash_command=f'"{PYTHON_BIN}" "{SCRIPT_MYSQL}"',
    )

    task_ingest_mongo_and_seed = BashOperator(
        task_id='ingest_reviews_to_mongo_and_seed',
        bash_command=f'"{PYTHON_BIN}" "{SCRIPT_MONGO}"',
    )

    task_dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && "{DBT_BIN}" run',
    )

    [task_ingest_mysql, task_ingest_mongo_and_seed] >> task_dbt_run