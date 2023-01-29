from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import pandas as pd
import os
import psycopg2, psycopg2.extras

dag = DAG(
    dag_id='553_postgresql_export_dag',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}

###POSTGRESQL settings###
#set postgresql connectionfrom basehook
pg_conn = BaseHook.get_connection('pg_connection')

##init test connection
conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()

def load_file_to_pg(filename,pg_table,conn_args):

    df = pd.read_csv(f"/lessons/5. Реализация ETL в Airflow/4. Extract как подключиться к хранилищу, чтобы получить файл/Задание 2/{filename}" )
    #df=df.drop('id', axis=1)
    #df=df.drop_duplicates(subset=['uniq_id'])

    cols = ','.join(list(df.columns))
    insert_stmt = f"INSERT INTO stage.{pg_table} ({cols}) VALUES %s"

    conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
    cur = conn.cursor()

    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    conn.commit()

    cur.close()
    conn.close()


load_customer_research = PythonOperator(task_id='load_customer_research',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename':'customer_research.csv', 'pg_table':'customer_research', 'conn_args':conn},
                                        dag=dag)

load_user_order_log = PythonOperator(task_id='load_user_order_log',
                                     python_callable=load_file_to_pg,
                                     op_kwargs={'filename':'user_order_log.csv', 'pg_table':'user_order_log', 'conn_args':conn},
                                     dag=dag)

load_user_activity_log = PythonOperator(task_id='load_user_activity_log',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename':'user_activity_log.csv', 'pg_table':'user_activity_log', 'conn_args':conn},
                                        dag=dag)

load_customer_research >> load_user_order_log >> load_user_activity_log