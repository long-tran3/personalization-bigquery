with flatten_data as (
select  clientId, visitStartTime, visitId,
--         ARRAY_AGG(distinct (select  productSKU as product from unnest(hits.product) limit 2)) as product,177805
--         ARRAY_CONCAT_AGG(ARRAY(select productSKU as product from unnest(hits.product))) as product,
--         ARRAY(select productSKU as product from unnest(hits.product)) as product,
        hits.eCommerceAction.action_type as action, 
        hits_ctd.value as customerId,
        cast(products.productSKU as INT64) as sku
from `tiki-gap.122443995.ga_sessions_*` as log,
        unnest(hits) as hits,
        unnest(hits.product) as products,
        unnest(hits.customDimentions) AS hits_ctd
where hits_ctd.index = 1 and  
_table_suffix between '20200125' and '20200128' and hits.eCommerceAction.action_type = '3'
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
)

select clientId, customerId, visitStartTime, visitId, array_agg(distinct sku) as child_products, count(distinct sku) as num_prods
from flatten_data
group by clientId, visitStartTime, visitId, customerId order by num_prods

-- select  userId, 
        -- hits.page.pagePath as page, 
        -- hits.product.productSKU as product,
        -- hits.eCommerceAction.action_type as action
-- from `tiki-gap.122443995.ga_sessions_*` as log,
        -- unnest(hits) as hits
-- where  _table_suffix between '20200125' and '20200128'
-- limit 10


select clientId, visitStartTime, visitId, count(hits) from `tiki-gap.122443995.ga_sessions_*` as log,
        unnest(hits) as hits
where _table_suffix between '20200125' and '20200128'
group by clientId, visitStartTime, visitId on count(hits) = 1

