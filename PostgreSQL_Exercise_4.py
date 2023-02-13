import time 
import json 
from airflow import DAG 
from airflow.operators.postgres_operator import PostgresOperator 
from datetime import timedelta
from airflow.utils.dates import days_ago

default_dag_args = { 'owner': 'airflow',
                'retries': 1,
                'retry_delay': timedelta(minutes=5)}



create_query = """
DROP TABLE IF EXIST public.employee
CREATE TABLE public.employee AS (id INT NOT NULL, name VARCHAR(250) , age INT);

"""

insert_data_query = """

INSERT INTO public.employee (id, name ,age) VALUES (1 , 'Cenk' , 23), (2, 'Javier' , 25), (3, 'Burak', 24);

"""


calculating_average_age ="""

SELECT AVG(age) 
FROM public.employee;

"""


dag_postgress = DAG(dag_id = 'postgress_dag_connection' , default_args = default_dag_args, schedule_interval= None, start_date= days_ago(1))

create_table = PostgresOperator(task_id = 'creation_of_table', sql = create_query, dag = dag_postgress, postgress_conn_id = 'postgress_cenk_local')

insert_data = PostgresOperator(task_id = 'insertion_of_data', sql = insert_data_query, dag = dag_postgress, postgress_conn_id = 'postgress_cenk_local')

group_data = PostgresOperator(task_id = 'calculating_average_age', sql = calculating_average_age, dag = dag_postgress, postgress_conn_id = 'postgress_cenk_local')

create_table >> insert_data >> group_data