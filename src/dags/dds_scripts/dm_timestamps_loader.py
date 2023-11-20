from datetime import datetime, date, time
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
    status_ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class StgReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, rank_threshold: int) -> List[ObjModel]:
        with self._db.client().cursor(row_factory=class_row(ObjModel)) as cur:
            cur.execute(
                """
                    select
                        ts_stg.id,
                        ts_stg.status_ts,
                        extract('year' from ts_stg.status_ts) as "year",
                        extract('month' from ts_stg.status_ts) as "month",
                        extract('day' from ts_stg.status_ts) as "day",
                        ts_stg.status_ts::time as "time",
                        ts_stg.status_ts::date as "date"
                    from (select
			                id,
			                replace(((json_array_elements(object_value::json->'statuses'))->'dttm')::varchar,'"','')::timestamp as status_ts
	                    from stg.ordersystem_orders oo
		                    where id > %(threshold)s
                            order by id asc) ts_stg;
                """, {
                    "threshold": rank_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class DdsInserter:

    def insert_row(self, conn: Connection, object: ObjModel) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (
                        %(ts)s, %(year_ts)s,%(month_ts)s,%(day_ts)s,%(ts)s::time,%(ts)s::date
                    );
                """,
                {
                    "ts": object.status_ts,
                    "year_ts": object.year,
                    "month_ts": object.month,
                    "day_ts": object.day,
                    "time_ts": object.time,
                    "date_ts": object.date
                },
            )


class TimestampLoader:
    WF_KEY = "dm_timestamps_stg_to_dds_workflow"
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
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
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
