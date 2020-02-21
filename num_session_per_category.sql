with flatten_data as (
    select  clientId, visitStartTime, visitNumber,
            hits.eCommerceAction.action_type as action, 
            hits_ctd.value as customerId,
            safe_cast(products.productSKU as INT64) as sku
    from `tiki-gap.122443995.ga_sessions_*` as log,
            unnest(hits) as hits,
            unnest(hits.product) as products,
            unnest(hits.customDimensions) AS hits_ctd
    where hits_ctd.index = 1 and  
    _table_suffix between '20200101' and '20200210' and hits.eCommerceAction.action_type in ('2', '3', '6') and safe_cast(products.productSKU as INT64) is not null
),

select clientId, date_trunc( date (timestamp_seconds(visitStartTime)), day) as day_time,
array_agg(sku) over (partition by clientId order by visitStartTime)
array_agg(sku) over (partition by clientId order by visitStartTime) 
from flatten_data

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
    select d.*, ccp.category_id as primary_category
    from flatten_data_join_main_category as d
    JOIN `tiki-dwh.ecom.catalog_category_product` AS ccp ON d.sku = ccp.product_id AND ccp.is_primary = 1
),

final as (
    select clientId, visitStartTime, customerId, visitNumber, array_agg(distinct sku) as child_products, 
        array_agg(distinct category) main_category, array_agg(distinct primary_category) primary_category, count(distinct sku) as num_prods,
        count(distinct category) num_main_category, count(distinct primary_category) num_primary_category
    from flatten_data_join_all_category
    group by clientId, visitStartTime, visitNumber, customerId order by num_primary_category
),

 temp as (
     select distinct clientId, visitNumber, customerId, primary_category,
      Dense_RANK() OVER ( PARTITION BY clientId, customerId ORDER BY visitNumber ) AS rank_session
    from flatten_data_join_all_category
    order by clientId, rank_session
),

data_with_session_diff as (
    select clientId, customerId, primary_category, rank_session,
    IFNULL(rank_session - LAG(rank_session) OVER (PARTITION BY clientId, customerId, primary_category ORDER BY rank_session), 0) AS session_diff
    from temp
), 

session_info as (
    select clientId, customerId, primary_category, count(*) as sum_session, countif(session_diff != 1) as diff_continous_session
    from data_with_session_diff
    group by clientId, customerId, primary_category
    order by sum_session desc
),

hehe as (
    select clientId, customerId, primary_category, sum_session/diff_continous_session as average_session_used, cc.name from 
    session_info as si
    join `tiki-dwh.ecom.catalog_category` as cc
    on si.primary_category = cc.id
    order by average_session_used desc
)

select primary_category, name, avg(average_session_used) as average_session_used from hehe group by primary_category, name order by average_session_used desc
-- select sum(sum_session), sum(diff_continous_session) from session_info where diff_continous_session >= 2
-- select sum(sum_session), sum(diff_continous_session) from session_info



-- 34482 > 1
-- 692403 = 0
-- 144158 = 1
-- 871043


