import logging
import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
args = {"owner": "airflow", "start_date": airflow.utils.dates.days_ago(1)}
dag = DAG(
 dag_id="External_Second", default_args=args, schedule_interval='55 06 * * *'
)
def pp():
 print('Second Dependent Task')
 
with dag:
 Second_Task=PythonOperator(task_id="Second_Task", python_callable=pp,dag=dag)
 
ExternalTaskSensor(
 task_id='Ext_Sensor_Task',
 external_dag_id='External_First',
 external_task_id='first_task',
 execution_delta = timedelta(minutes=10),
 timeout=300,
 dag=dag)>>Second_Task