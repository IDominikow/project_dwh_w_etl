import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg_scripts.delivery_system_couriers_loader import CouriersLoader
from stg_scripts.delivery_system_deliveries_loader import DeliveriesLoader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 11, 9, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project', 'api', 'stg','couriers','deliveries'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def origin_to_stg_delivery_system_load():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_couriers")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CouriersLoader(dwh_pg_connect, log)
        rest_loader.load_from_api()  # Вызываем функцию, которая перельет данные.  

    @task(task_id="load_deliveries")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveriesLoader(dwh_pg_connect, log)
        rest_loader.load_from_api()  # Вызываем функцию, которая перельет данные.  

    coureirs_loader = load_couriers()
    deliveries_loader = load_deliveries()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    [load_couriers, deliveries_loader]  # type: ignore


order_stg_dag = origin_to_stg_delivery_system_load()  # noqa
