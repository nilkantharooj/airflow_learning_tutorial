from airflow import DAG # type: ignore
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator # type: ignore
from random import randint
from airflow.operators.bash import BashOperator # type: ignore

def _choose_best_model(ti):
    accuracy= ti.xcom_pull(task_ids=[
        'training_model_a',
        'training_model_b',
        'training_model_c'
    ])
    best_accuracy = max(accuracy)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'

def _training_model():
    return randint(1,10)

with DAG(
    "my_dag",
    start_date = datetime(2025,3,22),
    schedule_interval = "@daily",
    catchup = False
) as dag:
    
    training_model_a = PythonOperator(
        task_id = "training_model_a",
        python_callable = _training_model
    )

    training_model_b = PythonOperator(
        task_id = "training_model_b",
        python_callable = _training_model
    )

    training_model_c = PythonOperator(
        task_id = "training_model_c",
        python_callable = _training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id = "choose_best_model",
        python_callable = _choose_best_model
    )

    accurate = BashOperator(
        task_id = "accurate",
        bash_command = "echo 'Accurate'"
    )

    inaccurate = BashOperator(
        task_id = "inaccurate",
        bash_command = "echo 'Inaccurate'"
    )

    [training_model_a, training_model_b, training_model_c] >> choose_best_model >> [accurate, inaccurate]


    
  