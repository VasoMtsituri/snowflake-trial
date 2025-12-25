"""
An example Airflow DAG that uses Cosmos to run dbt models against a Snowflake data warehouse.
"""
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# --- Configuration ---
# Point to the directory where your dbt project lives
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/.local/bin/dbt")
DAG_FOLDER = Path(__file__).parent
DBT_PROJECT_PATH = DAG_FOLDER / "project_1"

# Define how Cosmos should map Airflow connections to dbt profiles
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"database": "ALPHA_VANTAGE_STAGING", "schema": "ALPHA_VANTAGE"},
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
    start_date=datetime(2025, 12, 25),
    catchup=False,
    dag_id="snowflake_dbt_cosmos_dag",
)
