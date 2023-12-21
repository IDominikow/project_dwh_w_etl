from logging import Logger
from typing import List

from stg_scripts.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
import requests
import json
from datetime import datetime


class ApiReader:
    def list_deliveries(self,limit: int,sort_field: str, sort_direction: str,from_ts: datetime, to_ts: datetime, headers_auth : str):
        offset = 0
        response_list = []
        while True:
            response_del = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?from={from_ts}&to={to_ts}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
                            headers = json.loads(headers_auth)
                            ).json()
            offset+=limit
            response_list += response_del
            if len(response_del) == 0:
                
                break
        return response_list
        
    def list_couriers(self,limit: int, offset: int,sort_field: str, sort_direction: str, headers_auth : str):
        response_list = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
                headers = json.loads(headers_auth)
                ).json()
        return response_list
    


class PgInserter:

    def insert_deliveries(self, conn: Connection, jsons_list: list) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_system_deliveries (object_id , object_value, upload_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(upload_ts)s);
                """,
                {   
                    "object_id": jsons_list["order_id"],
                    "object_value": json.dumps(jsons_list, ensure_ascii=False),
                    "upload_ts": datetime.now() 
                },
            )

    def insert_couriers(self, conn: Connection, jsons_list: list) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_system_couriers (object_id , object_value, upload_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(upload_ts)s);
                """,
                {   
                    "object_id": jsons_list["_id"],
                    "object_value": json.dumps(jsons_list, ensure_ascii=False),
                    "upload_ts": datetime.now() 
                },
            )


class DeliveriesLoader:
    WF_KEY = "delivery_system_deliveries_load_workflow"
    LAST_LOADED_TS = "last_loaded_ts"
    limit = 50
    sort_field = "date"
    sort_direction = "asc"
    to_ts = datetime.now().replace(microsecond=0)


    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ApiReader()
        self.stg = PgInserter()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_from_api(self, headers_auth):
        self.headers_auth = headers_auth
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=1, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS: datetime(2023,1,1)})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS]

            load_queue = self.origin.list_deliveries(DeliveriesLoader.limit, DeliveriesLoader.sort_field , DeliveriesLoader.sort_direction, last_loaded, DeliveriesLoader.to_ts, headers_auth)
            upload_ts = datetime.now()
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting: no deliveries to load.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.stg.insert_deliveries( conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS] = upload_ts
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS]}")

class CouriersLoader:
    WF_KEY = "delivery_system_couriers_load_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    limit = 50
    sort_field = "_id"
    sort_direction = "asc"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ApiReader()
        self.stg = PgInserter()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_from_api(self, headers_auth):
        self.headers_auth = headers_auth
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=1, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.origin.list_couriers(CouriersLoader.limit,last_loaded,CouriersLoader.sort_field,CouriersLoader.sort_direction, headers_auth)

            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting: no couriers to load.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.stg.insert_couriers(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded+len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
