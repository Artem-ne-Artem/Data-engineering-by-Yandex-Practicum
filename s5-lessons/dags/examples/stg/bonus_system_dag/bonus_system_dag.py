import logging

import pendulum
from airflow.decorators import dag, task
from examples.config_const import ConfigConst
from lib import ConnectionBuilder
from examples.stg.bonus_system_dag.event_loader import EventLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
)
def sprint5_case_stg_bonus_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
    origin_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_ORIGIN_BONUS_SYSTEM_CONNECTION)

    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    events = load_events()

    events  # type: ignore


# type: ignore


stg_bonus_system_dag = sprint5_case_stg_bonus_system_dag()
