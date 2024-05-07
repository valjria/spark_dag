from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%y-%m-%d')

print(yesterday_date)

default_args = {
    'owner': 'train',
    'start_date': yesterday_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG ('simple_spark_dag', default_args = default_args, schedule_interval='*/5 * * * *', catchup=False, template_searchpath=["/home/train/pythonProject/dags/sql_files"]) as dag:

    t1 = BashOperator(task_id="download_data", bash_command='wget https://raw.githubusercontent.com/erkansirin78/datasets/master/dirty_store_transactions.csv -O /tmp/dirty_store_transactions.csv')
    #t3 = BashOperator(task_id='check_file_exists', bash_command='sha256sum /tmp/dirty_store_transactions.csv')

    t2= FileSensor(task_id='check_file_exists', filepath='/tmp/dirty_store_transactions.csv')

    t3 = SSHOperator(task_id='clean_Dirty_data', ssh_conn_id='my_ssh_conn',command='source /home/train/venvspark/bin/activate; '
                                                                                   'spark-submit --master local /home/train/pythonProject/dags/scripts/spark_dirty_data_cleaner.py')

    t4 = PostgresOperator(task_id="create_table", postgres_conn_id="my_postgresql_conn", sql="create_clean_trns_limited.sql")

    t5 = PostgresOperator(task_id="insert_records", postgres_conn_id="my_postgresql_conn", sql="insert_into_clean_trns.sql")


    t1 >> t2 >> t3 >> t4 >> t5







