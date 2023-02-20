import logging
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.config_const import ConfigConst
from lib import ConnectionBuilder

from examples.cdm.deployer.settlement_report import SettlementReportLoader
from examples.cdm.deployer.courier_report import CourierLedgerLoader
from examples.cdm.deployer.schema_init import SchemaDdl

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'settlement'],
    is_paused_upon_creation=False
)
def cdm_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
    # Забираем путь до каталога с SQL-файлами из переменных Airflow.
    ddl_path = Variable.get("CDM_DDL_FILES_PATH")

    @task
    def schema_init():
        schema_ddl = SchemaDdl(dwh_pg_connect, log)
        schema_ddl.init_schema(ddl_path)

    @task
    def settlement_daily_report_load():
        rest_loader = SettlementReportLoader(dwh_pg_connect)
        rest_loader.load_report_by_days()

    @task
    def courier_ledger_report_load():
        rest_loader = CourierLedgerLoader(dwh_pg_connect)
        rest_loader.load_report_by_month()

    # Инициализируем объявленные таски.
    init_schema = schema_init()
    settlement_daily_report_load = settlement_daily_report_load()
    courier_ledger_report_load = courier_ledger_report_load()

    (
    init_schema >> settlement_daily_report_load >> courier_ledger_report_load
    )

cdm_dag = cdm_dag()  # noqa