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
    courier_id: str
    courier_name: str




class StgReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, threshold: int) -> List[ObjModel]:
        with self._db.client().cursor(row_factory=class_row(ObjModel)) as cur:
            cur.execute(
                """
                    select 
	                    id,
	                    object_id as courier_id,
	                    replace((object_value->'name')::varchar,'"','') as courier_name
	                from stg.delivery_system_couriers dsc
                        where id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                        order by id ASC; --Обязательна сортировка по id, т.к. id используем в качестве курсора.
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
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name
                        ;
                """,
                {
                    "courier_id": object.courier_id,
                    "courier_name": object.courier_name
                },
            )


class CourierLoader:
    WF_KEY = "dm_couriers_stg_to_dds_workflow"
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
            self.log.info(f"Found {len(load_queue)} couriers to load.")
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
