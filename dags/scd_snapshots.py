from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime 

# ... (default_args definition)

with DAG(
    dag_id="SCD2_snapshots_banking",
    # ... (DAG metadata)
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:
    
    # Task 1: Run dbt run command for the 'staging' selection
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        # Use the verified absolute path: /home/airflow/.local/bin/dbt
        bash_command="cd /opt/airflow/dbt_for_banking && dbt run --select staging --profiles-dir /home/airflow/.dbt"
    )


    # Task 2: Run dbt snapshot command
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        # Use the verified absolute path: /home/airflow/.local/bin/dbt
        bash_command="cd /opt/airflow/dbt_for_banking && dbt snapshot --profiles-dir /home/airflow/.dbt"
    )

    # Task 3: Run dbt run command for the 'marts' selection
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        # Use the verified absolute path: /home/airflow/.local/bin/dbt
        bash_command="cd /opt/airflow/dbt_for_banking && dbt run --select marts --profiles-dir /home/airflow/.dbt"
    )

    # Set Ta
    # sk Dependency
    dbt_staging >> dbt_snapshot >> dbt_run_marts
    
