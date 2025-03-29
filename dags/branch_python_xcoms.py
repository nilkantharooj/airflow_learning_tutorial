from airflow.decorators import dag, task # type: ignore
from datetime import datetime

@dag("branching_xcoms",
    start_date = datetime(2024,3,29),
    schedule = "@daily",
    catchup = False
)

def branching():
    @task
    def ml_a():
        return 7
    
    @task.branch(do_xcom_push = False)
    def choose_best_model(accuracy):
        if accuracy>6:
            return 'is_accurate'
        return 'is_inaccurate'
    
    @task
    def is_accurate():
        pass

    @task
    def is_inaccurate():
        pass

    @task(trigger_rule='none_failed_min_one_success')
    def store_data():
        pass

    choose_best_model(ml_a()) >> [is_accurate(), is_inaccurate()] >> store_data()

branching()