import time
import requests
import json
import pandas as pd
import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

# POSTGRES_CONN_ID = 'postgresql_de'
# NICKNAME = 'ddd.z.2000'
# COHORT = '8'

NICKNAME = Variable.get("nickname")
COHORT = Variable.get("cohort")
POSTGRES_CONN_ID = Variable.get("postgres_conn_id")

headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    task_logger.info('Запрос generate_report')
    # print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    task_logger.info('Запрос get_report')
    # print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError('Превышен срок ожидания ответа от сервера')

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    task_logger.info('Запрос get_increment')
    # print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')

    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    task_logger.info('Загрузка в staging')

    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{increment_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename, index_col=False)
    #df = df.drop('id', axis=1)
    df = df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

# Вместо df.to_sql можно попробовать написать sql запрос с командой COPY и тогда pandas вообще не понадобится
    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    # row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    row_count = engine.execute(f'COPY {pg_schema}.{pg_table} FROM PROGRAM \'curl {s3_filename}\' DELIMITER \',\' CSV HEADER ')
    print(f'{row_count} rows was inserted')


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4
}

business_dt = '{{ ds }}'

with DAG(
        'sales_mart',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=1),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    dimension_tasks = list()
    for i in ['d_city', 'd_item', 'd_customer']:
        dimension_tasks.append(PostgresOperator(
            task_id=f'load_{i}',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=f'sql/mart.{i}.sql',
            dag=dag
            )
        )
    dimension_tasks = list()

    # update_d_item_table = PostgresOperator(
    #     task_id='update_d_item',
    #     postgres_conn_id=POSTGRES_CONN_ID,
    #     sql="sql/mart.d_item.sql")
    #
    # update_d_customer_table = PostgresOperator(
    #     task_id='update_d_customer',
    #     postgres_conn_id=POSTGRES_CONN_ID,
    #     sql="sql/mart.d_customer.sql")
    #
    # update_d_city_table = PostgresOperator(
    #     task_id='update_d_city',
    #     postgres_conn_id=POSTGRES_CONN_ID,
    #     sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}}
    )

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            #>> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> dimension_tasks
            >> update_f_sales
            >> update_f_customer_retention
    )