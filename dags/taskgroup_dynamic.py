from airflow import DAG # type: ignore
from airflow.models.baseoperator import chain # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

with DAG('taskgroup_dynamic', 
  start_date=datetime(2023, 1, 1), 
  schedule='@daily', 
  catchup=False):

    start = BashOperator(task_id='start', bash_command='exit 0')
    with TaskGroup(group_id='paths') as paths:
       for gid in {'a', 'b', 'c'}:
           with TaskGroup(group_id=f'path_{gid}') as path:
              task_process  = BashOperator(task_id=f'task_process_{gid}', bash_command='exit 0')
              task_store  = BashOperator(task_id=f'task_store_{gid}', bash_command='exit 0')
              task_process >> task_store
    end = BashOperator(task_id='end', bash_command='exit 0')

start >> paths >> end