from logging import Logger
from typing import List

from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ObjModel(BaseModel):
    status_name: str


class StgReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self) -> List[ObjModel]:
        with self._db.client().cursor(row_factory=class_row(ObjModel)) as cur:
            cur.execute(
                """
                    select distinct 
	                    replace(((json_array_elements(object_value::json->'statuses'))->'status')::varchar,'"','') as status_name
	                from stg.ordersystem_orders oo;
                """
            )
            objs = cur.fetchall()
        return objs

class DdsInserter:

    def insert_row(self, conn: Connection, object: ObjModel) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_statuses(status_name)
                    VALUES (%(status_name)s)
                    ON CONFLICT (status_name) DO NOTHING;
                """,
                {
                    "status_name": object.status_name
                },
            )


class StatusLoader:

    def __init__(self, pg_origin: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_origin
        self.origin = StgReader(pg_origin)
        self.stg = DdsInserter()
        self.log = log

    def load_from_stg(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            load_queue = self.origin.list_objects()
            self.log.info(f"Found {len(load_queue)} statuses to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.stg.insert_row(conn, row)

