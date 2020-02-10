with
sale_order as (
SELECT customer_id, UNIX_SECONDS(created_at) as time
FROM `tiki-dwh.ecom.sales_order`
where created_at is not null and created_at >= "2019-06-01" and created_at is not null and customer_id is not null
),

sale_order_timediff as (
select customer_id, time,
ifnull(time - lag(time) over (partition by customer_id order by time), 0) as time_diff
from sale_order
order by customer_id, time
),

temp as (
select distinct(customer_id) from sale_order_timediff
where customer_id not in (
    select distinct customer_id
    from sale_order_timediff
    where time_diff > 604800
) and time_diff != 0
order by customer_id
),

haha as (

select distinct customer_id, date_trunc(date (created_at), month) as time_trunc
from `tiki-dwh.ecom.sales_order`
where created_at is not null and created_at >= "2019-01-01" 
and created_at is not null and customer_id is not null
order by time_trunc
)

select customer_id, count(*) as nMonths from haha group by customer_id order by nMonths on 


-- select distinct(customer_id) from sale_order_timediff


-- 2651968
