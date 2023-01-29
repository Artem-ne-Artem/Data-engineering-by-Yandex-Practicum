import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from sqlalchemy import create_engine

# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
}


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    engine = create_engine('postgresql+psycopg2://jovyan:jovyan@localhost:5432/de')
    df.to_sql('titanic', engine, index=False, if_exists='replace', schema='public')


def pivot_dataset():
    engine = create_engine('postgresql+psycopg2://jovyan:jovyan@localhost:5432/de')
    titanic_df = pd.read_sql('select * from public.titanic', con=engine)

    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()

    df.to_sql('titanic_pivot', engine, index=False, if_exists='replace', schema='public')


dag = DAG(
    dag_id='titanic_pivot',  # Имя DAG
    schedule_interval=None,  # Периодичность запуска, например, "00 15 * * *"
    default_args=args,  # Базовые аргументы
)

# BashOperator, выполняющий указанную bash-команду
start = BashOperator(
    task_id='start',
    bash_command='echo "Here we start! "',
    dag=dag,
)

# Загрузка датасета
create_titanic_dataset = PythonOperator(
    task_id='download_titanic_dataset',
    python_callable=download_titanic_dataset,
    dag=dag,
)

# Чтение, преобразование и запись датасета
pivot_titanic_dataset = PythonOperator(
    task_id='pivot_dataset',
    python_callable=pivot_dataset,
    dag=dag,
)

# Порядок выполнения задач
start >> create_titanic_dataset >> pivot_titanic_dataset