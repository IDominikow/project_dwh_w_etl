import logging

import pendulum
from airflow.decorators import dag, task
from dds_scripts.dm_users_loader import UserLoader
from dds_scripts.dm_restaurants_loader import RestaurantLoader
from dds_scripts.dm_timestamps_loader import TimestampLoader
from dds_scripts.dm_products_loader import ProductLoader
from dds_scripts.dm_orders_loader import OrderLoader
from dds_scripts.fct_product_sales_loader import ProductSalesLoader
from dds_scripts.dm_couriers_loader import CourierLoader
from dds_scripts.dm_statuses_loader import StatusLoader
from dds_scripts.dm_order_statuses_loader import OrderStatusesLoader
from dds_scripts.fct_order_deliveries_loader import OrderDeliveriesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_to_dds_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.

    # Объявляем таск, который загружает данные.
    @task(task_id="load_users")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_restaurants")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.
    
    @task(task_id="load_timestamps")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_products")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_orders")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.
         
    @task(task_id="load__product_sales") 
    def load_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductSalesLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.
    
    @task(task_id="load_couriers") 
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_statuses") 
    def load_statuses():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = StatusLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_order_statuses") 
    def load_order_statuses():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderStatusesLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_order_deliveries") 
    def load_order_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderDeliveriesLoader(dwh_pg_connect, log)
        rest_loader.load_from_stg()

    # Инициализируем объявленные таски.
    load_users = load_users()
    load_restaurants = load_restaurants()
    load_timestamps = load_timestamps()
    load_products = load_products()
    load_orders = load_orders()
    load_product_sales = load_product_sales()
    load_couriers = load_couriers()
    load_statuses = load_statuses()
    load_order_statuses = load_order_statuses()
    load_order_deliveries = load_order_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [load_users, load_restaurants, load_timestamps,load_couriers, load_statuses] >> load_products >> load_orders >> load_order_statuses >> [load_product_sales, load_order_deliveries] # type: ignore


stg_bonus_system_users_dag = stg_to_dds_load_dag()