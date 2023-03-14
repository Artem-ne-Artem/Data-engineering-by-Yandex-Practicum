from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import boto3
import pendulum

aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

def fetch_s3_file(bucket: str, key: str) -> None:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    s3_client.download_file(Bucket=bucket, Key=key, Filename=f'/data/{key}')


# эту команду надо будет поправить, чтобы она выводила первые десять строк каждого файла
# bash_command_tmpl = """echo {{ params.files }}"""

@dag(schedule_interval = None, start_date = pendulum.parse('2022-07-13'))
def project_6_load_to_folder():
    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]

tasks = project_6_load_to_folder()