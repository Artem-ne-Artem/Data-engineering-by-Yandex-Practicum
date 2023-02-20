import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib import ConnectionBuilder, MongoConnect, ApiConnect
from examples.config_const import ConfigConst

from examples.stg.deployer.pg_bonus_ranks_loader import RankLoader
from examples.stg.deployer.pg_bonus_users_loader import UsersLoader
from examples.stg.deployer.pg_bonus_event_loader import EventLoader
from examples.stg.deployer.mg_restaurants_loader import MG_RestaurantLoader, RestaurantOriginRepository, RestaurantDestRepository
from examples.stg.deployer.mg_users_loader import MG_UserLoader, UserOriginRepository, UserDestRepository
from examples.stg.deployer.mg_orders_loader import MG_OrderLoader, OrderOriginRepository, OrderDestRepository
from examples.stg.deployer.schema_init import SchemaDdl
from examples.stg.deployer.rest_couriers_loader import REST_CourierLoader, CourierDestRepository, CourierOriginRepository
from examples.stg.deployer.rest_deliveries_loader import REST_DeliveryLoader, DeliveryDestRepository, DeliveryOriginRepository

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/30 * * * *',       # Задаем расписание выполнения дага - каждый 30 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),     # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,      # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'schema', 'ddl'],     # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False       # Остановлен/запущен при появлении. Сразу запущен.
)

def stg_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Забираем путь до каталога с SQL-файлами из переменных Airflow.
    ddl_path = Variable.get("STG_DDL_FILES_PATH")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Получаем переменные из Airflow для подключения к MONGO_DB.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    origin_mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

    # Объявляем таск, который создает структуру таблиц.
    @task(task_id="schema_init")
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)

    @task(task_id="pg_ranks_load")
    def pg_load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()

    @task(task_id="pg_users_load")
    def pg_load_users():
        user_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()

    @task(task_id="pg_events_load")
    def pg_load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    @task(task_id="mg_restaurants_load")
    def mg_load_restaurants():
        restaurants_pg_saver = RestaurantDestRepository()
        restaurants_collection_reader = RestaurantOriginRepository(origin_mongo_connect)
        restaurants_loader = MG_RestaurantLoader(restaurants_collection_reader, dwh_pg_connect, restaurants_pg_saver,
                                                 log)
        restaurants_loader.load_restaurant()

    @task(task_id="mg_users_load")
    def mg_load_users():
        users_pg_saver = UserDestRepository()
        users_collection_reader = UserOriginRepository(origin_mongo_connect)
        users_loader = MG_UserLoader(users_collection_reader, dwh_pg_connect, users_pg_saver, log)
        users_loader.load_user()

    @task(task_id="mg_orders_load")
    def mg_load_orders():
        orders_pg_saver = OrderDestRepository()
        orders_collection_reader = OrderOriginRepository(origin_mongo_connect)
        orders_loader = MG_OrderLoader(orders_collection_reader, dwh_pg_connect, orders_pg_saver, log)
        orders_loader.load_order()

    @task(task_id="rest_couriers_load")
    def rest_couriers_load():
        couriers_pg_saver = CourierDestRepository()
        couriers_collection_reader = CourierOriginRepository()
        couriers_loader = REST_CourierLoader(couriers_collection_reader, dwh_pg_connect, couriers_pg_saver, log)
        couriers_loader.load_courier()

    @task(task_id="rest_deliveries_load")
    def rest_deliveries_load():
        deliveries_pg_saver = DeliveryDestRepository()
        deliveries_collection_reader = DeliveryOriginRepository()
        deliveries_loader = REST_DeliveryLoader(deliveries_collection_reader, dwh_pg_connect, deliveries_pg_saver, log)
        deliveries_loader.load_delivery()


    # Инициализируем объявленные таски.
    init_schema = schema_init()
    pg_ranks = pg_load_ranks()
    pg_users = pg_load_users()
    pg_events = pg_load_events()
    mg_restaurants = mg_load_restaurants()
    mg_users = mg_load_users()
    mg_orders = mg_load_orders()
    rest_couriers = rest_couriers_load()
    rest_deliveries = rest_deliveries_load()

    init_schema # type: ignore
    pg_ranks  # type: ignore
    pg_users  # type: ignore
    pg_events  # type: ignore
    mg_restaurants  # type: ignore
    mg_users  # type: ignore
    mg_orders  # type: ignore
    rest_couriers
    rest_deliveries

stg_dag = stg_dag()