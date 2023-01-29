# Создайте новый DAG, состоящий из трёх шагов.
# Start — начало процесса, пустой шаг-заглушка.
# Группа из трёх сенсоров, которые проверяют наличие файлов ['customer_research.csv','user_order_log.csv','user_activity_log.cs] в локальной папке.
# Учтите, что имена файлов в локальной папке отличаются. Назовите шаг task_id по шаблону task_id="waiting_for_file_имя_файла".
# Finish — окончание процесса, пустой шаг-заглушка.

from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# def get_files_from_s3(business_dt,s3_conn_name):
#     BUCKET_NAME = 'my-bucket'
#     s3 = boto3.resource(s3_conn_name)
#     for s3_filename in ['customer_research.csv', 'user_order_log.csv', 'user_activity_log.csv']:
#         local_filename = business_dt.replace('-','_') + '_' + s3_filename # add date to filename
#
#     s3.Bucket(BUCKET_NAME).download_file(s3_filename, local_filaname)

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow"
}

with DAG(
        dag_id="Sprin4_Task32",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False
) as dag:

    with TaskGroup(group_id='group1') as fg1:
        f1 = FileSensor(
            task_id = 'waiting_for_file_customer_research',  # имя задачи
            fs_conn_id = 'fs_local',  # имя соединения
            filepath = str(datetime.now().date()) + 'customer_research.csv',  # путь к файлу
            poke_interval = 10
        )
        f2 = FileSensor(
            task_id = 'waiting_for_file_user_order_log',  # имя задачи
            fs_conn_id = 'fs_local',  # имя соединения
            filepath = str(datetime.now().date()) + 'user_order_log.csv',  # путь к файлу
            poke_interval = 10
        )
        f3 = FileSensor(
            task_id = 'waiting_for_file_user_activity_log',  # имя задачи
            fs_conn_id = 'fs_local',  # имя соединения
            filepath = str(datetime.now().date()) + 'user_activity_log.csv',  # путь к файлу
            poke_interval = 10
        )

    fg1
