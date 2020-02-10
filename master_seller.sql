with tmp_catalog_product as (
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
)

select * from master_vs_seller where master_id = child_id limit 10

