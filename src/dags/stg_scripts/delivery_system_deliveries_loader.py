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
    def list_jsons(self,limit: int, offset: int,sort_field: str, sort_direction: str, to_ts: datetime, from_ts: datetime):
        response_list = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?from={from_ts}&to={to_ts}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
                        headers = { "X-Nickname" : "sergey-sushko" ,
                        "X-Cohort": "17",
                        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"}
                        ).json()
        return response_list
    


class PgInserter:

    def insert_rows(self, conn: Connection, jsons_list: list) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_system_deliveries(object_id , object_value, upload_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(upload_ts)s);
                """,
                {
                    "object_id": jsons_list["order_id"],
                    "object_value": json.dumps(jsons_list, ensure_ascii=False),
                    "upload_ts": datetime.now(),
                },
            )


class DeliveriesLoader:
    WF_KEY = "delivery_system_deliveries_load_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    limit = 50
    sort_field = "date"
    sort_direction = "asc"
    to_ts = datetime.now().replace(microsecond=0)
    from_ts = datetime(2023,1,1)

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ApiReader()
        self.stg = PgInserter()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_from_api(self):
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

            load_queue = self.origin.list_jsons(DeliveriesLoader.limit, last_loaded, DeliveriesLoader.sort_field , DeliveriesLoader.sort_direction, DeliveriesLoader.to_ts, DeliveriesLoader.from_ts)

            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting: no deliveries to load.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.stg.insert_rows(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded+len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
