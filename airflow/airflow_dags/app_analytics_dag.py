import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# --- Environment Setup (Final Correct Version for Codespaces) ---
PROJECT_ROOT = "/workspaces/apppulse-elt-project"

# This is the correct path to the Python executable INSIDE our virtual environment
VENV_PYTHON_BIN = os.path.join(PROJECT_ROOT, "venv", "bin", "python")

# dbt is globally available thanks to pipx, so we can call it directly
DBT_BIN = "dbt"

DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "app_dbt")
SCRIPT_MYSQL = os.path.join(PROJECT_ROOT, "scripts", "ingest_apps_to_mysql.py")
SCRIPT_MONGO = os.path.join(PROJECT_ROOT, "scripts", "ingest_reviews_to_mongodb.py")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 22),
    'retries': 0, # We set retries to 0 for easier debugging
}

with DAG(
    dag_id='app_pulse_analytics_pipeline',
    default_args=default_args,
    description='Automates the AppPulse ELT pipeline',
    schedule_interval=None, # We will trigger it manually
    catchup=False,
    tags=['apppulse-final'],
) as dag:

    task_ingest_mysql = BashOperator(
        task_id='ingest_apps_to_mysql',
        bash_command=f'"{VENV_PYTHON_BIN}" "{SCRIPT_MYSQL}"',
    )

    task_ingest_mongo_and_seed = BashOperator(
        task_id='ingest_reviews_to_mongo_and_seed',
        bash_command=f'"{VENV_PYTHON_BIN}" "{SCRIPT_MONGO}"',
    )

    task_dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && {DBT_BIN} run',
    )

    [task_ingest_mysql, task_ingest_mongo_and_seed] >> task_dbt_run