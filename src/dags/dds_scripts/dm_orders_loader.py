from logging import Logger
from typing import List

from dds_scripts.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel, Field
from typing import Optional


class ObjModel(BaseModel):
    id: int
    order_key: str
    restaurant_id: int
    user_id: int
    courier_id: Optional[int] = Field(  ...,nullable = True)


class StgReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, rank_threshold: int) -> List[ObjModel]:
        with self._db.client().cursor(row_factory=class_row(ObjModel)) as cur:
            cur.execute(
                """
                    select
		                oo.id,
		                replace((oo.object_value::json->'_id')::varchar,'"','') as order_key,
		                dr.id  as restaurant_id,
		                du.id as user_id,
       	                dc.id as courier_id
		            from stg.ordersystem_orders oo
                        join dds.dm_restaurants dr on replace(((oo.object_value::json ->'restaurant')->'id')::varchar,'"','') = dr.restaurant_id
                        join dds.dm_users du on replace(((oo.object_value::json ->'user')->'id')::varchar,'"','') = du.user_id
                        left join stg.delivery_system_deliveries dsd on dsd.object_id = replace((oo.object_value::json->'_id')::varchar,'"','')
                        left join dds.dm_couriers dc on replace(((dsd.object_value::json ->'courier_id'))::varchar,'"','') = dc.courier_id
                    WHERE oo.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY oo.id ASC; --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": rank_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class DdsInserter:

    def insert_row(self, conn: Connection, order: ObjModel) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key , user_id, restaurant_id, courier_id)
                    VALUES (%(order_key)s, %(user_id)s, %(restaurant_id)s, %(courier_id)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        courier_id = EXCLUDED.courier_id
                        ;
                """,
                {
                    "order_key": order.order_key,
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "courier_id": order.courier_id
                },
            )


class OrderLoader:
    WF_KEY = "dm_orders_stg_to_dds_workflow"
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
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.stg.insert_row(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
