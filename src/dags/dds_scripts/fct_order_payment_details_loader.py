from logging import Logger
from typing import List

from dds_scripts.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ObjModel(BaseModel):
    id: int
    order_id: int
    order_sum: float
    delivery_rate: float
    delivery_tip_sum: float




class StgReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, threshold: int) -> List[ObjModel]:
        with self._db.client().cursor(row_factory=class_row(ObjModel)) as cur:
            cur.execute(
                """
                    select 
	                    dsd.id,
	                    do2.id as order_id,
	                    (dsd.object_value::json->'sum')::varchar::numeric as order_sum,
	                    (dsd.object_value::json->'rate')::varchar::numeric as delivery_rate,
	                    (dsd.object_value::json->'tip_sum')::varchar::numeric as delivery_tip_sum
	                from stg.delivery_system_deliveries dsd
	                    join dds.dm_orders do2 on do2.order_key = dsd.object_id
                    where dsd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    order by dsd.id asc; --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": threshold
                }
            )
            objs = cur.fetchall()
        return objs


class DdsInserter:

    def insert_row(self, conn: Connection, object: ObjModel) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_order_payment_details(order_id, order_sum, delivery_rate, delivery_tip_sum)
                    VALUES (%(order_id)s, %(order_sum)s, %(delivery_rate)s, %(delivery_tip_sum)s)
                        ;
                """,
                {
                    "order_id": object.order_id,
                    "order_sum": object.order_sum,
                    "delivery_rate": object.delivery_rate,
                    "delivery_tip_sum": object.delivery_tip_sum
                },
            )


class OrderPaymentsLoader:
    WF_KEY = "fct_order_payment_details_load_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_origin
        self.origin = StgReader(pg_origin)
        self.stg = DdsInserter()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_from_stg(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=1, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_objects(last_loaded)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for sale in load_queue:
                self.stg.insert_row(conn, sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
