import logging

import pendulum
from airflow.decorators import dag, task
from cdm_scripts.dm_settlement_report_loader import SettlementReportLoader
from cdm_scripts.dm_courier_ledger_report_loader import CourierLedgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['cdm','from_dds','settlement_report',"courier_ledger_report"],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def cdm_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.

    # Объявляем таск, который загружает данные.
    @task(task_id="load__settlement_report")
    def load_settlement_report():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = SettlementReportLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_from_dds()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_courier_ledger_report")
    def load_courier_ledger_report():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_from_dds()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    load_courier_ledger_report = load_courier_ledger_report()
    load_settlement_report = load_settlement_report()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [load_settlement_report, load_courier_ledger_report] # type: ignore


stg_bonus_system_users_dag = cdm_load_dag()