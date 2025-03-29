from airflow.decorators import dag, task # type: ignore
from airflow.sensors.python import PythonSensor # type: ignore
from airflow.exceptions import AirflowSensorTimeout, AirflowFailException # type: ignore
from datetime import datetime

def _done():
  pass

def _partner_a():
  return False

def _partner_b():
  return True

def _failure_callback(context):
  if isinstance(context['exception'], AirflowSensorTimeout):
    print(context)
    print("Sensor timed out")

@dag("sensor_dag",
  start_date=datetime(2023, 1, 1),
  schedule='@daily',
  catchup=False
)
def my_dag():

  waiting_for_a = PythonSensor(
    task_id='waiting_for_a',
    poke_interval=120,
    timeout=10,
    mode='reschedule',
    python_callable=_partner_a,
    on_failure_callback=_failure_callback,
    soft_fail=True
  )

  waiting_for_b = PythonSensor(
    task_id='waiting_for_b',
    poke_interval=120,
    timeout=10,
    mode='reschedule',
    python_callable=_partner_b,
    on_failure_callback=_failure_callback,
    soft_fail=True
  )

  @task(trigger_rule='none_failed_min_one_success')
  def done():
    pass

  [waiting_for_a, waiting_for_b] >> done()

my_dag()