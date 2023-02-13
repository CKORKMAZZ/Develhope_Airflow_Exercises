from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_dag_args = {

    'start_date' : datetime(2022, 1, 1),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes= 5),
    'project_id' : 1
}

with DAG('Bash_Commands', schedule_interval= None, default_args = default_dag_args) as bash_dag:
    task_0 = BashOperator(task_id = 'Bash_task', bash_command = "echo 'Command Executed from Bash Operator'")
    task_1 = BashOperator(task_id = 'Bash_task_move_data', bash_command = "cp /opt/airflow/data/DATA_CENTER/DATA_LAKE/dataset_raw.txt /opt/airflow/data/DATA_CENTER/CLEAN_DATA/")
    task_2 = BashOperator(task_id = 'Bash_task_delete_data', bash_command = 'rm /opt/airflow/data/DATA_CENTER/DATA_LAKE/dataset_raw.txt')

    task_0 >> task_1 >> task_2
