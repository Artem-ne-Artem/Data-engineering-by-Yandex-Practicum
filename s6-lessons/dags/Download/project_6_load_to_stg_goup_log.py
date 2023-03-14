import contextlib
import hashlib
import json
from typing import Dict, List, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
import pandas as pd
import pendulum
import vertica_python


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
            print(f"loading rows {start}-{end}")
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
    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',
            'schema': 'DDDZ2000YANDEXRU__STAGING',
            'table': 'group_log',
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'],
        },
    )

    start >> load_group_log >> end


project_6_load_to_stg = project_6_load_to_stg()

# df_group_log['user_id_from'] = pd.array(df_group_log['user_id_from'], dtype="Int64")
