import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# --- Environment Setup (Final Version - Running dbt from venv) ---
PROJECT_ROOT = "/workspaces/apppulse-elt-project"
VENV_PATH = os.path.join(PROJECT_ROOT, "venv")
VENV_PYTHON_BIN = os.path.join(VENV_PATH, "bin", "python")
DBT_BIN = os.path.join(VENV_PATH, "bin", "dbt") # dbt executable inside venv

DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "app_dbt")
WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "warehouse")
SCRIPT_MYSQL = os.path.join(PROJECT_ROOT, "scripts", "ingest_apps_to_mysql.py")
SCRIPT_MONGO = os.path.join(PROJECT_ROOT, "scripts", "ingest_reviews_to_mongodb.py")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 22),
    'retries': 0,
}

with DAG(
    dag_id='app_pulse_analytics_pipeline',
    default_args=default_args,
    description='Automates the AppPulse ELT pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['apppulse-final-venv'],
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
        # --- FINAL COMMAND: Activate venv before running dbt seed and run ---
        bash_command=(
            f'source {VENV_PATH}/bin/activate && ' # Activate the virtual environment
            f'mkdir -p "{WAREHOUSE_DIR}" && '
            f'cd "{DBT_PROJECT_DIR}" && '
            # Run dbt seed using the venv dbt
            f'"{DBT_BIN}" seed --project-dir . --profiles-dir . && '
            # Run dbt run using the venv dbt
            f'"{DBT_BIN}" run --project-dir . --profiles-dir .'
        ),
    )

    [task_ingest_mysql, task_ingest_mongo_and_seed] >> task_dbt_run