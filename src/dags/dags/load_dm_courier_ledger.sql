with cd as (select 
        dc.id as courier_id,
        dc.courier_name  as courier_name,
        dt."year" as settlement_year,
        dt."month" as settlement_month,
        avg(fod.delivery_rate) as rate_avg
    from dds.fct_order_payment_details fod 
        join dds.dm_orders do2 on do2.id = fod.order_id 
        join dds.dm_order_statuses dos on do2.id = dos.order_id 
        join dds.dm_statuses ds on dos.status_id = ds.id 
        join dds.dm_timestamps dt on dos.timestamp_id = dt.id
        join dds.dm_couriers dc on do2.courier_id = dc.id
    where ds.status_name = 'OPEN'
        group by dc.id, courier_name,settlement_year, settlement_month
    ), 
    full_cte as(select 
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
    from dds.fct_order_payment_details fod 
        join dds.dm_orders do2 on do2.id = fod.order_id 
        join dds.dm_order_statuses dos on do2.id = dos.order_id 
        join dds.dm_statuses ds on dos.status_id = ds.id 
        join dds.dm_timestamps dt on dos.timestamp_id = dt.id
        join dds.dm_couriers dc on do2.courier_id = dc.id
        join cd on (cd.courier_id, cd.courier_name, cd.settlement_year, cd.settlement_month) = (dc.id, dc.courier_name, dt."year", dt."month")
    where ds.status_name = 'OPEN' 
    and dt."year" = extract('year' from now()) and
    dt."month" = extract('month' from now())
        group by dc.id, dc.courier_name,dt."year", dt."month", cd.rate_avg)
  INSERT INTO 
        cdm.dm_courier_ledger(
            courier_id,courier_name, 
            settlement_year, 
            settlement_month, 
            orders_count, 
            orders_total_sum, 
            rate_avg, 
            order_processing_fee, 
            courier_order_sum, 
            courier_tips_sum, 
            courier_reward_sum)
        select 
            courier_id,
            courier_name, 
            settlement_year, 
            settlement_month,
            orders_count, 
            orders_total_sum, 
            rate_avg, 
            orders_processing_fee, 
            courier_order_sum,
            courier_tips_sum, 
            courier_reward_sum 
            from full_cte
    on conflict(courier_id, courier_name, settlement_year, settlement_month)
    do update set
        orders_count = EXCLUDED.orders_count,
        orders_total_sum = EXCLUDED.orders_total_sum,
        rate_avg = EXCLUDED.rate_avg,
        order_processing_fee = EXCLUDED.order_processing_fee,
        courier_order_sum = EXCLUDED.courier_order_sum,
        courier_tips_sum = EXCLUDED.courier_tips_sum,
     courier_reward_sum = EXCLUDED.courier_reward_sum;   