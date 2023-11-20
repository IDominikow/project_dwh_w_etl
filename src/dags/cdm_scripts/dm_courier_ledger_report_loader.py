from logging import Logger
from typing import List
from datetime import datetime, date

from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ObjModel(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    orders_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float

class DdsReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self) -> List[ObjModel]:
        with self._db.client().cursor(row_factory=class_row(ObjModel)) as cur:
            cur.execute(
                """
                with cd as (select 
	                dc.id as courier_id,
                    dc.courier_name  as courier_name,
                    dt."year" as settlement_year,
                    dt."month" as settlement_month,
                    avg(fod.delivery_rate) as rate_avg
                from dds.fct_order_deliveries fod 
                    join dds.dm_orders do2 on do2.id = fod.order_id 
                    join dds.dm_order_statuses dos on do2.id = dos.order_id 
                    join dds.dm_statuses ds on dos.status_id = ds.id 
                    join dds.dm_timestamps dt on dos.timestamp_id = dt.id
                    join dds.dm_couriers dc on do2.courier_id = dc.id
                where ds.status_name = 'OPEN'
                    group by dc.id, courier_name,settlement_year, settlement_month
                )select 
	                dc.id as courier_id,
                    dc.courier_name  as courier_name,
                    dt."year" as settlement_year,
                    dt."month" as settlement_month,
                    count(fod.id)as orders_count,
                    sum(fod.order_sum) as orders_total_sum,
                    cd.rate_avg as rate_avg,
                    sum(fod.order_sum)*0.25 as orders_processing_fee,
                    sum(case 
                        when cd.rate_avg < 4 then greatest(fod.order_sum*0.05,100)
                        when cd.rate_avg >=4 and cd.rate_avg < 4.5 then greatest(fod.order_sum*0.07,150)
                        when cd.rate_avg >=4.5 and cd.rate_avg < 4.9 then greatest(fod.order_sum*0.08,175)
                        when cd.rate_avg >=4.9 then greatest(fod.order_sum*0.1,200)
                        end) as courier_order_sum,
                    sum(fod.delivery_tip_sum) as courier_tips_sum,
                    sum(case 
                                when cd.rate_avg < 4 then greatest(fod.order_sum*0.05,100)
                                when cd.rate_avg >=4 and cd.rate_avg < 4.5 then greatest(fod.order_sum*0.07,150)
                                when cd.rate_avg >=4.5 and cd.rate_avg < 4.9 then greatest(fod.order_sum*0.08,175)
                                when cd.rate_avg >=4.9 then greatest(fod.order_sum*0.1,200)
                                end) + (sum(fod.delivery_tip_sum)*0.95) as courier_reward_sum
                from dds.fct_order_deliveries fod 
                    join dds.dm_orders do2 on do2.id = fod.order_id 
                    join dds.dm_order_statuses dos on do2.id = dos.order_id 
                    join dds.dm_statuses ds on dos.status_id = ds.id 
                    join dds.dm_timestamps dt on dos.timestamp_id = dt.id
                    join dds.dm_couriers dc on do2.courier_id = dc.id
                    join cd on (cd.courier_id, cd.courier_name, cd.settlement_year, cd.settlement_month) = (dc.id, dc.courier_name, dt."year", dt."month")
                where ds.status_name = 'OPEN'
                    group by dc.id, dc.courier_name,dt."year", dt."month", cd.rate_avg
                """, 
            )
            objs = cur.fetchall()
        return objs


class CdmInserter:

    def insert_row(self, conn: Connection, object: ObjModel) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id,courier_name, settlement_year , settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, 
                    courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s,
                    %(orders_processing_fee)s, %(courier_order_sum)s,%(courier_tips_sum)s, %(courier_reward_sum)s)
                    on conflict(courier_id, courier_name, settlement_year, settlement_month)
                    do update set
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    rate_avg = EXCLUDED.rate_avg,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum
                    ;
                """,
                {   
                "courier_id": object.courier_id,
                "courier_name": object.courier_name,
                "settlement_year": object.settlement_year,
                "settlement_month": object.settlement_month,
                "orders_count": object.orders_count,
                "orders_total_sum": object.orders_total_sum,
                "rate_avg": object.rate_avg,
                "orders_processing_fee": object.orders_processing_fee,
                "courier_order_sum": object.courier_order_sum,
                "courier_tips_sum": object.courier_tips_sum,
                "courier_reward_sum": object.courier_reward_sum
                },
            )


class CourierLedgerLoader:


    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DdsReader(pg_origin)
        self.stg = CdmInserter()
        self.log = log

    def load_from_dds(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            load_queue = self.origin.list_objects()

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.stg.insert_row(conn, row)

