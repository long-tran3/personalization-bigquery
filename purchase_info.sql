SELECT
  soi.product_name, soi.created_at
FROM
  `tiki-dwh.ecom.sales_order` as so
  join 
  `tiki-dwh.ecom.sales_order_item` as soi
  on so.id = soi.order_id
  where so.customer_id = 1985247
  
  
--   select soi.product_name, so.customer_id, cc.location_city
--   from `tiki-dwh.ecom.sales_order_item` as soi
--   join `tiki-dwh.ecom.sales_order` as so
--   on so.id = soi.order_id 
--   join `tiki-dwh.cdp.customers` as cc
--   on so.customer_id = safe_cast(cc.customer_id as int64)
--   where product_name like '' and cc.location_city = 'Hà Nội'
  
-- 1185949: hien
