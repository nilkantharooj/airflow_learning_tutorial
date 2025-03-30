from airflow import DAG # type: ignore
from airflow.example_dags.subdags.subdag import subdag # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore
from airflow.operators.subdag import SubDagOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore

DAG_NAME = 'example_subdag_operator'

with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 2},
    start_date=days_ago(2),
    schedule_interval="@once",
    tags=['example'],
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag(DAG_NAME, 'section-1', dag.default_args),
    )

    some_other_task = EmptyOperator(
        task_id='some-other-task',
    )

    section_2 = SubDagOperator(
        task_id='section-2',
        subdag=subdag(DAG_NAME, 'section-2', dag.default_args),
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> section_1 >> some_other_task >> section_2 >> end