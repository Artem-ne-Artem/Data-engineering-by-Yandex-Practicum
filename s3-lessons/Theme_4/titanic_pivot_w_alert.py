import datetime as dt
import pandas as pd

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from sqlalchemy import create_engine  # Для подключения к БД

POSTGRES_URL = Variable.get('postgres_url')

args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
}

# нужные функции для обработки данных
def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    engine = create_engine(POSTGRES_URL)
    df.to_sql('titanic', engine, index=False, if_exists='replace', schema='public')


def pivot_dataset():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    titanic_df = pd.read_sql('select * from public.titanic', con=engine)

    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()

    df.to_sql('titanic_pivot', engine, index=False, if_exists='replace', schema='public')


dag = DAG(dag_id='titanic_pivot_w_alert',  # Имя DAG
          schedule_interval=None,  # Периодичность запуска, например, "00 15 * * *"
          default_args=args,  # Базовые аргументы
          )

# BashOperator, выполняющий указанную bash-команду
first_task = BashOperator(
    task_id='first_task',
    bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)

with TaskGroup(group_id="preprocessing_stage", dag=dag) as preprocessing:

    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        dag=dag,
    )
    # Чтение, преобразование и запись датасета
        pivot_titanic_dataset = PythonOperator(
        task_id='pivoting_dataset',
        python_callable=pivot_dataset,
        dag=dag,
    )
    create_titanic_dataset >> pivot_titanic_dataset

first_task >> preprocessing