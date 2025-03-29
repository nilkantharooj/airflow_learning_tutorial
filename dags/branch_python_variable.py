from airflow.models import Variable # type: ignore
from airflow.decorators import dag, task # type: ignore
from datetime import datetime

ml_b = Variable.get("ml_b")
ml_c = Variable.get("ml_c")

@dag("branching_variable",
    start_date = datetime(2024,3,29),
    schedule = "@daily",
    catchup = False
)

def branching():
    @task
    def max_accuracy():
        return max(int(ml_b), int(ml_c))
    
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
    
    choose_best_model(max_accuracy()) >> [is_accurate(), is_inaccurate()] >> store_data()

branching()