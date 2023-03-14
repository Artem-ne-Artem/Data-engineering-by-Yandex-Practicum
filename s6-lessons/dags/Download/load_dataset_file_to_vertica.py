from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum
from vertica_python import connect

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"


def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(Bucket=bucket, Key=key, Filename=f'/data/{key}')


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_data():
    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]

    fetch_tasks

# def count_to_users():
#     with vertica_python.connect(**conn_info) as connection:
#         with connection.cursor() as cur:
#             cur.execute(f'''
#                 select
#                         count(id) cnt,
#                         count(DISTINCT(id)) unic_id,
#                         'users' dataset
#                 from LELIKOV2YANDEXRU__STAGING.users
#                 '''
#             )
#             # вывод результата в лог
#             result = cur.fetchall()
#             logging.info(f"SQL result: {result}")

def sprint6_stg_tables():
    db_connection = connect(host='51.250.75.20'
                            , port=5433
                            , user='DDDZ2000YANDEXRU'
                            , password='sIjgINZywlB6O1f'
                            , database='dwh'
                            , unicode_error='replace')

    cursor = db_connection.cursor()

    cursor.execute("""COPY DDDZ2000YANDEXRU__STAGING.dialogs 
                     (message_id, message_ts, message_from, message_to, message, message_group) 
                      from LOCAL '/data/dialogs.csv' DELIMITER ',';"""
                   )
    cursor.execute("""COPY DDDZ2000YANDEXRU__STAGING.groups 
                     (id, admin_id, group_name, registration_dt, is_private) 
                      from LOCAL '/data/groups.csv' DELIMITER ',';"""
                   )
    cursor.execute("""COPY DDDZ2000YANDEXRU__STAGING.users 
                     (id, chat_name, registration_dt, country, age) 
                      from LOCAL '/data/users.csv' DELIMITER ',';"""
                   )


# stg_csv = sprint6_dag_get_data()
# stg_tables = sprint6_stg_tables()

# stg_csv >> stg_tables