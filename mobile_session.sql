WITH
-- add_to_cart data 
atc_data AS (
    SELECT event_name, user_pseudo_id as fullVisitorId,  
    SAFE_CAST(event_timestamp / 1000 AS INT64) AS time,
    SAFE_CAST(user_id AS int64) AS user_id,
    (SELECT SAFE_CAST(eventParams.value.string_value AS int64)  FROM unnest(event_params) 
        AS eventParams WHERE eventParams.key='item_id') AS product_id,
    2 as action
    FROM
    `tikiandroid-1047.analytics_153801291.events_*`
    WHERE
    event_name = 'add_to_cart'
    AND user_id NOT LIKE '0'
    AND _TABLE_SUFFIX BETWEEN '20200130' AND '20200201'
),
-- product click data
pc_data AS (
SELECT event_name, user_pseudo_id as fullVisitorId,  
    SAFE_CAST(event_timestamp / 1000 AS INT64) AS time,
    SAFE_CAST(user_id AS int64) AS user_id,
    (SELECT SAFE_CAST(eventParams.value.string_value AS int64)  FROM unnest(event_params) 
        AS eventParams WHERE eventParams.key='spid') AS product_id,
    1 as action
    FROM
    `tikiandroid-1047.analytics_153801291.events_*`
    WHERE
    event_name = 'product_click'
    AND user_id NOT LIKE '0'
    AND _TABLE_SUFFIX BETWEEN '20200130' AND '20200201'
),
-- ecom purchase data
ep_data AS (
    with 
    sale_order as (
        select increment_id, entity_id from `tiki-dwh.ecom.sales_order_*`
        where _table_suffix between '20200130' AND '20200201'
    ),
    sale_order_item as (
        select order_id, product_id from `tiki-dwh.ecom.sales_order_item_*`
        where _Table_suffix between '20200130' AND '20200201'
    )

    SELECT event_name, user_pseudo_id as fullVisitorId,
    SAFE_CAST(event_timestamp / 1000 AS INT64) AS time,
    SAFE_CAST(user_id AS INT64) as user_id,
    (SELECT SAFE_CAST(eventParams.value.string_value AS int64)  FROM unnest(event_params) 
        AS eventParams WHERE eventParams.key='transaction_id') AS product_id,
    3 as action
    FROM
    `tikiandroid-1047.analytics_153801291.events_*`
    LEFT JOIN sale_order on sale_order.increment_id = (select value.string_value from unnest(event_params) where key = 'transaction_id')
    JOIN sale_order_item on sale_order_item.order_id  = sale_order.entity_id
    JOIN `tiki-dwh.dwh.dim_product_full` AS pi ON pi.product_key = product_id
    WHERE
    event_name = 'ecom_purchase'
    AND user_id NOT LIKE '0'
    AND _TABLE_SUFFIX BETWEEN '20200130' AND '20200201'
)

all_mobile_data AS (
    select * from atc_data 
    union all
    select * from pc_data
),

data_with_time_diff as (
    select *, 
    ifnull(time - LAG(time) over (partition by fullVisitorId order by time), 0) as time_diff
    from all_mobile_data where product_id is not null
    order by fullVisitorId, time
)

select * from all_mobile_data
