from airflow import DAG # type: ignore
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator # type: ignore
from random import randint
from airflow.operators.bash import BashOperator # type: ignore
from airflow.decorators import task # type: ignore


with DAG(
    "my_dag_optimized",
    start_date = datetime(2025,3,22),
    schedule = "@daily",
    description = "Training ML Models",
    tags = ["data engineering"],
    catchup = False
) as dag:
    
    @task
    def training_model(accuracy):
        return accuracy
    
    @task.branch
    def choose_best_model(accuracies):
        best_accuracy = max(accuracies)
        if (best_accuracy > 8):
            return 'accurate'
        return 'inaccurate'

    accurate = BashOperator(
        task_id = "accurate",
        bash_command = "echo 'Accurate'"
    )

    inaccurate = BashOperator(
        task_id = "inaccurate",
        bash_command = "echo 'Inaccurate'"
    )

    choose_best_model(training_model.expand(accuracy=[5,10,6])) >> [accurate, inaccurate]
    


    
  