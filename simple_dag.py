from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%y-%m-%d')

print(yesterday_date)

default_args = {
    'owner': 'train',
    'start_date': yesterday_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG ('simple_dag', default_args = default_args, schedule_interval='@daily', catchup=False) as dag:

    t1 = BashOperator(task_id='ls_tmp',bash_command='ls -l /tmp') #retries, retry_delay gibi ayarlar airflowda default var ordan çekiyor istersen belirt













