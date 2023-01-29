import datetime
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom

###API settings###
# set api connection from basehook
api_conn = BaseHook.get_connection('create_files_api')
# d5dg1j9kt695d30blp03.apigw.yandexcloud.net
api_endpoint = api_conn.host
# 5f55e6c0-e9e5-4a9c-b313-63c01fc31460
api_token = api_conn.password

# set user constants for api
nickname = 'ddd.z.2000'
cohort = '8'

headers = {
    "X-API-KEY": api_conn.password,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}

###POSTGRESQL settings###
# set postgresql connectionfrom basehook
psql_conn = BaseHook.get_connection('pg_connection')

## init test connection
conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()


# 1. запрашиваем выгрузку файликов получаем в итоге стринг task_id идентификатор задачи выгрузки
# где то через 60 секунд должен сформироваться по другому пути ссылка на доступ к выгруженным файлам
def create_files_request(ti, api_endpoint, headers):
    method_url = '/generate_report'
    r = requests.post('https://' + api_endpoint + method_url, headers=headers)
    response_dict = json.loads(r.content)
    ti.xcom_push(key='task_id', value=response_dict['task_id'])
    print(f"task_id is {response_dict['task_id']}")
    return response_dict['task_id']


# 2. проверяем готовность файлов в success на выход получаем стринг идентификатор готового репорта который является
# ссылкой до файлов которые можем скачивать
def check_report(ti, api_endpoint, headers):
    task_ids = ti.xcom_pull(key='task_id', task_ids=['create_files_request'])
    task_id = task_ids[0]

    method_url = '/get_report'
    payload = {'task_id': task_id}

    # отчет выгружается 60 секунд минимум - самое простое в слип на 70 секунд уводим - делаем 4 итерации - если нет то пусть валится в ошибку
    # значит с выгрузкой что-то не так по api и надо идти разбираться с генерацией данных - и в жизни такая же фигня бывает
    for i in range(4):
        time.sleep(70)
        r = requests.get('https://' + api_endpoint + method_url, params=payload, headers=headers)
        response_dict = json.loads(r.content)
        print(i, response_dict['status'])
        if response_dict['status'] == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break

    # тут соответствуенно если report_id не объявлен то есть не было SUCCESS то в ошибку упадет и инженер идет разбираться почему
    # можно сделать красивее но время
    ti.xcom_push(key='report_id', value=report_id)
    print(f"report_id is {report_id}")
    return report_id


# 3. загружаем 3 файлика в таблички (таблички stage)
def upload_from_s3_to_pg(ti, nickname, cohort):
    report_ids = ti.xcom_pull(key='report_id', task_ids=['check_report'])
    report_id = report_ids[0]

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/{REPORT_ID}/{FILE_NAME}'

    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", cohort)
    personal_storage_url = personal_storage_url.replace("{NICKNAME}", nickname)
    personal_storage_url = personal_storage_url.replace("{REPORT_ID}", report_id)

    # insert to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # get custom_research
    df_customer_research = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "customer_research.csv"))
    df_customer_research.reset_index(drop=True, inplace=True)
    insert_cr = "insert into stage.customer_research (date_id, category_id, geo_id, sales_qty, sales_amt) VALUES {cr_val};"
    i = 0
    step = int(df_customer_research.shape[0] / 100)
    while i <= df_customer_research.shape[0]:
        print('df_customer_research', i, end='\r')

        cr_val = str([tuple(x) for x in df_customer_research.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_cr.replace('{cr_val}', cr_val))
        conn.commit()

        i += step + 1

    # get order log
    df_order_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv"))
    df_order_log.reset_index(drop=True, inplace=True)
    insert_uol = "insert into stage.user_order_log (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount) VALUES {uol_val};"
    i = 0
    step = int(df_order_log.shape[0] / 100)
    while i <= df_order_log.shape[0]:
        print('df_order_log', i, end='\r')

        uol_val = str([tuple(x) for x in df_order_log.drop(columns=['id', 'uniq_id'], axis=1).loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_uol.replace('{uol_val}', uol_val))
        conn.commit()

        i += step + 1

    # get activity log
    df_activity_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv"))
    df_activity_log.reset_index(drop=True, inplace=True)
    insert_ual = "insert into stage.user_activity_log (date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    i = 0
    step = int(df_activity_log.shape[0] / 100)
    while i <= df_activity_log.shape[0]:
        print('df_activity_log', i, end='\r')

        if df_activity_log.drop(columns=['id'], axis=1).loc[i:i + step].shape[0] > 0:
            ual_val = str([tuple(x) for x in df_activity_log.drop(columns=['id', 'uniq_id'], axis=1).loc[i:i + step].to_numpy()])[1:-1]
            cur.execute(insert_ual.replace('{ual_val}', ual_val))
            conn.commit()

        i += step + 1

    cur.close()
    conn.close()

    # понятно что можно обернуть в функцию но для времени описал 3 разными запросами просто для экономии
    return 200


# 4. обновляем таблички d по загруженными в stage
def update_mart_d_tables(ti):
    # connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(
        f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # d_calendar
    cur.execute('''
    delete from mart.d_calendar CASCADE;
    
    with all_dates as (
    select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time from stage.user_activity_log
        union
    select distinct to_date(date_time::TEXT,'YYYY-MM-DD') from stage.user_order_log
        union
    select distinct to_date(date_id::TEXT,'YYYY-MM-DD') from stage.customer_research
    order by date_time
     )

    INSERT INTO mart.d_calendar (date_id, fact_date, day_num, month_num, month_name,year_num)
    
    select  distinct ROW_NUMBER () OVER (ORDER BY date_time) as date_id
            ,date_time::DATE as fact_date								--дата из date_time из всех существующих таблиц - она сформирована в cte all_dates;
            ,date_part('day',date_time)::INTEGER as day_num			--это номер дня в дате fact_date;
            ,date_part('month',date_time)::INTEGER as month_num		--это номер месяца в дате fact_date;
            ,to_char(date_time, 'Month')::varchar(8) as month_name	--это наименование месяца в дате fact_date;
            ,date_part('year',date_time)::INTEGER as year_num			--это номер года в дате fact_date.
    from    all_dates
    ''')
    conn.commit()

    # d_customer
    cur.execute('''
    delete from mart.d_customer;

    INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)

    select		customer_id, first_name, last_name, max(city_id)
    from 		stage.user_order_log 
    where		customer_id not in (select distinct customer_id from mart.d_customer)
    group by	customer_id, first_name, last_name
    ''')
    conn.commit()

    # d_item
    cur.execute('''
    delete from mart.d_item;

    INSERT INTO mart.d_item  (item_id ,item_name)

    select		distinct item_id ,item_name 
    from		stage.user_order_log
    where		item_id not in (select distinct item_id from mart.d_item)
    ''')
    conn.commit()
    cur.close()
    conn.close()
    return 200


# 5. апдейт витринок (таблички f)
def update_mart_f_tables(ti):
    # connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(
        f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # f_activity
    cur.execute('''
    delete from mart.f_activity;

    INSERT INTO mart.f_activity  (activity_id, date_id, click_number)
    select		ual.action_id as activity_id 
                ,dc.date_id
                ,sum(ual.quantity) as click_number
    from		stage.user_activity_log as ual
    left join 	mart.d_update_f_salesndar as dc
                on to_date(ual.date_time::TEXT,'YYYY-MM-DD') = dc.fact_date
    group by 	1,2
    ''')
    conn.commit()

    # f_daily_sales
    cur.execute('''
    delete from mart.f_daily_sales;

    INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, price, quantity, payment_amount)
    select		
                dc.date_id
                ,uol.item_id
                ,uol.customer_id
                ,avg (uol.payment_amount / uol.quantity) as price
                ,sum(uol.quantity) as quantity
                ,sum(payment_amount) as payment_amount
    from 		stage.user_order_log as uol
    left join	mart.d_calendar as dc
                on to_date(uol.date_time::TEXT,'YYYY-MM-DD') = dc.fact_date
    group by	1,2,3
    ''')
    conn.commit()
    cur.close()
    conn.close()
    return 200


# Объявляем даг
dag = DAG(
    dag_id='591_full_dag',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

t_file_request = PythonOperator(
    task_id='create_files_request'
    , python_callable=create_files_request
    , op_kwargs={'api_endpoint': api_endpoint, 'headers': headers}
    , dag=dag
)

t_check_report = PythonOperator(
    task_id='check_report'
    , python_callable=check_report
    , op_kwargs={'api_endpoint': api_endpoint, 'headers': headers}
    , dag=dag
)

t_upload_from_s3_to_pg = PythonOperator(
    task_id='upload_from_s3_to_pg'
    , python_callable=upload_from_s3_to_pg
    , op_kwargs={'nickname': nickname, 'cohort': cohort}
    , dag=dag
)

t_update_mart_d_tables = PythonOperator(
    task_id='update_mart_d_tables'
    , python_callable=update_mart_d_tables
    , dag=dag)

t_update_mart_f_tables = PythonOperator(
    task_id='update_mart_f_tables'
    , python_callable=update_mart_f_tables
    , dag=dag
)

t_file_request >> t_check_report >> t_upload_from_s3_to_pg >> t_update_mart_d_tables >> t_update_mart_f_tables
