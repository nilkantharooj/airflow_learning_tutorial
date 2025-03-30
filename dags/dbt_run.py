from datetime import datetime
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig # type: ignore
from cosmos.profiles import SnowflakeUserPasswordProfileMapping # type: ignore
from pathlib import Path

profile_config = ProfileConfig(
    profile_name="demo_dbt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={
            "database": "airbnb",
            "schema": "dev"
            },
        )
    )


dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path = Path(f"{os.environ['AIRFLOW_HOME']}/dags/demo_dbt"),
    ),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 30),
    catchup=False,
    dag_id="dbt_snowflake_dag",)