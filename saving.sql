with flatten_data as (
select  clientId, visitStartTime, visitNumber,
        hits.eCommerceAction.action_type as action, 
        hits_ctd.value as customerId,
        safe_cast(products.productSKU as INT64) as sku,
        products.productSKU as sku_str,
from `tiki-gap.122443995.ga_sessions_*` as log,
        unnest(hits) as hits,
        unnest(hits.product) as products,
        unnest(hits.customDimensions) AS hits_ctd
where hits_ctd.index = 1 and  
_table_suffix between '20200101' and '20200210' and hits.eCommerceAction.action_type in ('2', '3', '6') and safe_cast(products.productSKU as INT64) is not null
),

flatten_data_join_main_category as (
    SELECT
        d.clientId, d.visitStartTime, d.visitNumber, d.action, d.customerId, d.sku,
        ccp.category_id AS category
    FROM flatten_data AS d
    JOIN `tiki-dwh.ecom.catalog_category_product` AS ccp ON d.sku = ccp.product_id
    JOIN `tiki-dwh.ecom.catalog_category` AS cc ON ccp.category_id = cc.id
    WHERE
        cc.parent_id = 2 AND
        cc.include_in_menu = 1 AND
        cc.children_count > 0
),

flatten_data_join_all_category as (
    select d.*, (cast (ccp.category_id as string)) as  primary_category
    from flatten_data_join_main_category as d
    JOIN `tiki-dwh.ecom.catalog_category_product` AS ccp ON d.sku = ccp.product_id AND ccp.is_primary = 1
),

category_day as (
    select clientId, date_trunc( date (timestamp_seconds(visitStartTime)), day) as day_time, array_to_string(array_agg(distinct primary_category), ",") as primary_category
    from flatten_data_join_all_category
    group by clientId, day_time
),

categoryDay_2daybefore as (
    select clientId, day_time, UNIX_SECONDS(timestamp (day_time)) as time_int64, primary_category,
    array_agg(primary_category) over (partition by clientId order by UNIX_SECONDS(timestamp (day_time)) range between 172800 preceding and 86400 preceding) as sku_2_day_ago,
    array_agg(primary_category) over (partition by clientId order by UNIX_SECONDS(timestamp (day_time)) range between 604800 preceding and 86400 preceding) as sku_7_day_ago,
    from category_day
    order by clientId, day_time
),

category_day_flat as (
    select clientId, date_trunc( date (timestamp_seconds(visitStartTime)), day) as day_time, primary_category
    from flatten_data_join_all_category
),

categoryDay_flat_2daybefore as (
    select clientId, day_time, UNIX_SECONDS(timestamp (day_time)) as time_int64, primary_category,
    array_to_string(array_agg(primary_category) over (partition by clientId order by UNIX_SECONDS(timestamp (day_time)) range between 172800 preceding and 86400 preceding), ",") as sku_2_day_ago,
    array_to_string(array_agg(primary_category) over (partition by clientId order by UNIX_SECONDS(timestamp (day_time)) range between 604800 preceding and 86400 preceding), ",") as sku_7_day_ago,
    array_to_string(array_agg(primary_category) over (partition by clientId order by UNIX_SECONDS(timestamp (day_time)) range between 1209600 preceding and 86400 preceding), ",") as sku_14_day_ago
    from category_day_flat
    order by clientId, day_time
)

-- select numDay = (select count(*) from category_day), 
    -- numDayWith2DayBefore = (select count(*) from categoryDay_2daybefore where sku_2_day_ago is not null),
    -- numDayWith7DayBefore = (select count(*) from categoryDay_2daybefore where sku_7_day_ago is not null)


-- select * from categoryDay_flat_2daybefore  
-- where sku_2_day_ago is not null and sku_2_day_ago 
-- order by clientId, day_time

select 
(select count(*) from categoryDay_flat_2daybefore) as total,
(select count(*) from categoryDay_flat_2daybefore where sku_2_day_ago like concat('%', primary_category, '%')) as twoDayBefore,
(select count(*) from categoryDay_flat_2daybefore where sku_7_day_ago like concat('%', primary_category, '%')) as sevenDayBefore,
(select count(*) from categoryDay_flat_2daybefore where sku_14_day_ago like concat('%', primary_category, '%')) as 14_DayBefore
