from airflow.decorators import dag, task # type: ignore
from datetime import datetime

@dag("branching_optimized",
    start_date = datetime(2024,3,29),
    schedule = "@daily",
    catchup = False
)

def branching():
    @task.branch
    def choose_best_model(accuracy=7):
        if accuracy>6:
            return 'is_accurate'
        return 'is_inaccurate'
    
    @task
    def is_accurate():
        pass

    @task
    def is_inaccurate():
        pass

    choose_best_model() >> [is_accurate(),is_inaccurate()]

branching()