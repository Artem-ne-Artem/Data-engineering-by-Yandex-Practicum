# Создайте FileSensor, который будет проверять присутствие файла /data/test.txt в локальной папке.
# Чтобы сделать FileSensor, напишите новый DAG из трёх шагов:
# Start — начало процесса, пустой шаг-заглушка;
# Сенсор FileSensor, который проверяет наличие файла test.txt в локальной папке;
# Finish — окончание процесса, пустой шаг-заглушка.

from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow"
}

with DAG(
        dag_id="Sprin4_Task1",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False
) as dag:
    waiting_for_file = FileSensor(
        task_id='check_file',  # имя задачи
        fs_conn_id='fs_local',  # имя соединения
        filepath='/data/test.txt',  # путь к файлу
        poke_interval=60
    )

    waiting_for_file