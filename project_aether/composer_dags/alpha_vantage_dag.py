from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# --- Configuration ---
# Point to the directory where your dbt project lives
DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/dbt/my_snowflake_project")
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/.local/bin/dbt")

# Define how Cosmos should map Airflow connections to dbt profiles
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default", # The ID of your Airflow Connection
        profile_args={"database": "MY_DB", "schema": "MY_SCHEMA"},
    ),
)

# --- DAG Definition ---
snowflake_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_PATH,
    ),
    operator_args={
        "install_deps": True,  # Installs dbt packages if needed
        "full_refresh": False,
    },
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=str(DBT_EXECUTABLE_PATH),
    ),
    # Standard Airflow DAG arguments
    schedule_interval=None,
    start_date=datetime(2025, 12, 21),
    catchup=False,
    dag_id="snowflake_dbt_cosmos_dag",
)