# Создайте оператор SQLCheckOperator.
# Текст запросов для оператора должен быть сохранён в файлах user_order_log_isNull_check.sql и user_activity_log_isNull_check.sql.

# Скрипты SQL
# select count(1) as qty from de.public.user_activity_log where customer_id is not null
# select count(1) as qty from de.public.user_order_log where customer_id is not null


from airflow import DAG
from datetime import datetime
from airflow.operators.sql import SQLCheckOperator

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"
}

with DAG(
        dag_id="Sprin4_Task1",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False
) as dag:
    sql_check = SQLCheckOperator(
        task_id='user_order_log_isNull',
        sql='user_order_log_isNull_check.sql',
        # params=,
    )
    sql_check2 = SQLCheckOperator(
        task_id='user_activity_log_isNull',
        sql='user_activity_log_isNull_check.sql',
        # params=,
    )

    sql_check >> sql_check2


