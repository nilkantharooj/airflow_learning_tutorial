from airflow import DAG # type: ignore
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

with DAG(
    "postgres_dag",
    start_date=datetime(2025,3,29),
    schedule="@daily",
    catchup=False
) as dag:
    
    create_table=SQLExecuteQueryOperator(
        task_id= 'create_table',
        conn_id = 'postgres_connection',
        sql = "create table test_data_1(id int);"
    )

    insert_data=SQLExecuteQueryOperator(
        task_id= 'insert_data',
        conn_id = 'postgres_connection',
        sql = "insert into test_data_1 values(1);"
    )

    query_data=SQLExecuteQueryOperator(
        task_id= 'query_data',
        conn_id = 'postgres_connection',
        sql = "select id from test_data_1;"
    )

    create_table >> insert_data >> query_data
    
