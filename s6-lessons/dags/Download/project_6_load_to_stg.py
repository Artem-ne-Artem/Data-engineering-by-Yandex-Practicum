import contextlib
import hashlib
import json
import logging
from typing import Dict, List, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
import pandas as pd
import pendulum
import vertica_python

log = logging.getLogger(__name__)

def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    conn_info = {'host': '51.250.75.20',
                 'port': '5433',
                 'user': 'DDDZ2000YANDEXRU',
                 'password': 'sIjgINZywlB6O1f',
                 'database': 'dwh',
                 # Вначале он нам понадобится, а дальше — решите позже сами
                 'autocommit': True
                 }

    vertica_conn = vertica_python.connect(**conn_info)
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            log.info(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()

@dag(
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13')
)
def project_6_load_to_stg():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/users.csv',
            'schema': 'DDDZ2000YANDEXRU__STAGING',
            'table': 'users',
            'columns': ['id', 'chat_name', 'registration_dt', 'country', 'age'], #, 'gender', 'email']
            'type_override': {'age': 'Int64'},
        },
    )

    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/groups.csv',
            'schema': 'DDDZ2000YANDEXRU__STAGING',
            'table': 'groups',
            'columns': ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private'],
        },
    )

    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/dialogs.csv',
            'schema': 'DDDZ2000YANDEXRU__STAGING',
            'table': 'dialogs',
            'columns': ['message_id', 'message_ts', 'message_from', 'message_to', 'message', 'message_group'],
        },
    )


    start >> [load_users, load_groups, load_dialogs] >> end


project_6_load_to_stg = project_6_load_to_stg()
