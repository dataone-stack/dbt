select order_id,total_amount,create_time,order_status,payment_method,shipping_carrier,ship_by_date,buyer_cancel_reason,days_to_ship, checkout_shipping_carrier,'Chaching' as brand from `chaching_shopee_shop_dwh.shopee_order_detail_chaching_brand`
union all
select order_id,total_amount,create_time,order_status,payment_method,shipping_carrier,ship_by_date,buyer_cancel_reason,days_to_ship, checkout_shipping_carrier,'LYB' as brand from `lyb_shopee_shop_dwh.shopee_order_detail_lyb_official`
union all
select order_id,total_amount,create_time,order_status,payment_method,shipping_carrier,ship_by_date,buyer_cancel_reason,days_to_ship, checkout_shipping_carrier,'An Cung' as brand from `ancung_shopee_shop_dwh.shopee_order_detail_ancung_brand`
union all
select order_id,total_amount,create_time,order_status,payment_method,shipping_carrier,ship_by_date,buyer_cancel_reason,days_to_ship, checkout_shipping_carrier,'LYB Cosmetics' as brand from `lybcosmetic_shopee_shop_dwh.shopee_order_detail_lyb_cosmetic`
union all
select order_id,total_amount,create_time,order_status,payment_method,shipping_carrier,ship_by_date,buyer_cancel_reason,days_to_ship, checkout_shipping_carrier,'UME' as brand from `ume_shopee_shop_dwh.shopee_order_detail_ume_viet_nam`
