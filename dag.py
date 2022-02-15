import airflow
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from airflow import DAG, macros
from airflow.decorators import task
import os

YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
MONTH = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'
YESTERDAY = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d") }}'

dag = DAG(
    dag_id='dag_groupe8',
    schedule_interval='0 0 * * *',
    max_active_runs=1,
    start_date=datetime(2022, 2, 14),
)

@task(task_id='download_raw_data', dag=dag)
def download_raw_data(year, month, day):
    for rm_path in ["/groupe8/clean/data.parquet", "/groupe8/clean/final.parquet"]:
        try:
            rm = Popen(["hdfs", "dfs", "-rm", rm_path], stdin=PIPE, bufsize=-1)
            rm.communicate()
        except:
            pass

    raw_data_path = "/groupe8/raw/data.csv"
    url = f'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv?refine=t_1h%3A{year}%2F{month}%2F{day}&timezone=UTC'
    r = requests.get(url, allow_redirects=True)
    open(f'/tmp/data-{year}-{month}-{day}.csv', 'wb').write(r.content)
    put = Popen(["hadoop", "fs", "-put", f"/tmp/data-{year}-{month}-{day}.csv", raw_data_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(f'/tmp/data-{year}-{month}-{day}.csv')
    print("ok")


download_raw_data = download_raw_data(YEAR, MONTH, YESTERDAY)

raw_path = 'hdfs:///groupe8/raw/data.csv'
clean_path = 'hdfs:///groupe8/clean/data.parquet'
joint_path = 'hdfs:///data/clean/trust'
out_path = 'hdfs:///groupe8/clean/final.parquet'

clean_data = BashOperator(
    task_id="spark_job_clean",
    bash_command=f"/usr/bin/spark-submit --class esgi.circulation.Clean /jars/groupe8/circulation.jar {raw_path} {clean_path}",
    dag=dag
)

transform_data = BashOperator(
    task_id="spark_job_transform",
    bash_command=f"/usr/bin/spark-submit --class esgi.circulation.Jointure /jars/groupe8/circulation.jar {clean_path} {joint_path} {out_path}",
    dag=dag
)

download_raw_data >> clean_data >> transform_data
