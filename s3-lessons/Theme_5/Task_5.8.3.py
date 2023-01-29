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
    dag_id='583_postgresql_mart_update',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt': '2022-05-06'}

###POSTGRESQL settings###
# set postgresql connectionfrom basehook
pg_conn = BaseHook.get_connection('pg_connection')

##init test connection - проверяем корректность соединения
conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()


# 3. обновляем таблички d по загруженными в stage
# Напишите функцию update_mart_d_tables, которая будет создавать подключение с хранилищем из airflow pg_connection
# и исполнять скрипт обновления таблиц d_*.
def update_mart_d_tables(ti):
    # connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
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
    select 
      distinct ROW_NUMBER () OVER (ORDER BY date_time) as date_id
      ,date_time::DATE as fact_date								--дата из date_time из всех существующих таблиц - она сформирована в cte all_dates;
      ,date_part('day',date_time)::INTEGER as day_num			--это номер дня в дате fact_date;
      ,date_part('month',date_time)::INTEGER as month_num		--это номер месяца в дате fact_date;
      ,to_char(date_time, 'Month')::varchar(8) as month_name	--это наименование месяца в дате fact_date;
      ,date_part('year',date_time)::INTEGER as year_num			--это номер года в дате fact_date.
    from all_dates
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


# 4. апдейт витринок (таблички f)
# Напишите функцию update_mart_f_tables, которая будет создавать подключение с хранилищем из airflow pg_connection
# и исполнять скрипт обновления таблиц f_*
def update_mart_f_tables(ti):
    # connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # f_activity
    cur.execute('''
    delete from mart.f_activity;

    INSERT INTO mart.f_activity  (activity_id, date_id, click_number)
    select		ual.action_id as activity_id 
                ,dc.date_id
                ,sum(ual.quantity) as click_number
    from		stage.user_activity_log as ual
    left join 	mart.d_calendar as dc
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


t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)

t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)

t_update_mart_d_tables >> t_update_mart_f_tables