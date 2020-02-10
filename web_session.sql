with flatten_data as (
select  clientId, visitStartTime, visitId,
        hits.eCommerceAction.action_type as action, 
        hits_ctd.value as customerId,
        safe_cast(products.productSKU as INT64) as sku
from `tiki-gap.122443995.ga_sessions_*` as log,
        unnest(hits) as hits,
        unnest(hits.product) as products,
        unnest(hits.customDimensions) AS hits_ctd
where hits_ctd.index = 1 and  
_table_suffix between '20200110' and '20200203' and hits.eCommerceAction.action_type = '6' and safe_cast(products.productSKU as INT64) is not null
),

flatten_data_join_main_category as (
    SELECT
        d.clientId, d.visitStartTime, d.visitId, d.action, d.customerId, d.sku,
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

tmp_catalog_product as (
    SELECT * FROM `tiki-dwh.ecom.catalog_product`
),

tmp_super_link as (
    SELECT * FROM `tiki-dwh.ecom.catalog_product_super_link`
),

master_simple_vs_seller_simple as (
    SELECT master_id, id as child_id
    FROM tmp_catalog_product
    WHERE master_id IN (
        SELECT id
        FROM tmp_catalog_product
        WHERE entity_type = "master_simple"
    )
),

master_conf_vs_seller_simple as (
    SELECT sl.parent_id as master_id, cp.id as child_id
    FROM tmp_catalog_product as cp
    JOIN tmp_super_link as sl
    ON cp.entity_type = "seller_simple"
    AND cp.master_id = sl.product_id

),

master_vs_seller as (
    SELECT *
    FROM master_simple_vs_seller_simple
    UNION ALL (SELECT * FROM master_conf_vs_seller_simple)
),

flatten_join_child as (
    select clientId, visitStartTime, visitId, sku 
    from flatten_data inner join master_vs_seller
    on flatten_data.sku = master_vs_seller.child_id
),


final as (
    select clientId, customerId, visitStartTime, visitId, array_agg(distinct sku) as child_products, 
        array_agg(distinct category) main_category, array_agg(distinct primary_category) primary_category, count(distinct sku) as num_prods,
        count(distinct category) num_main_category, count(distinct primary_category) num_primary_category
    from flatten_data_join_all_category
    group by clientId, visitStartTime, visitId, customerId order by num_primary_category
)

select * from final
select num_prods as product_buy_in_session, count(*) as num_session from final group by num_prods order by num_prods
select num_main_category as main_category_buy_in_session , count(*) as num_session from final group by main_category_buy_in_session order by main_category_buy_in_session
select num_primary_category as primary_category_buy_in_session , count(*) as num_session from final group by primary_category_buy_in_session order by primary_category_buy_in_session

-- all 248602





