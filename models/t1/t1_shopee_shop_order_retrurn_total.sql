
select order_id,update_time, item, 'Chaching' as brand from `chaching_shopee_shop_dwh.shopee_return_chaching_brand`
union all
select order_id,update_time, item, 'LYB' as brand from `lyb_shopee_shop_dwh.shopee_return_lyb_official`
union all
select order_id,update_time, item, 'An Cung' as brand from `ancung_shopee_shop_dwh.shopee_return_ancung_brand`
union all
select order_id,update_time, item, 'LYB Cosmetics' as brand from `lybcosmetic_shopee_shop_dwh.shopee_return_lyb_cosmetic`
union all
select order_id,update_time, item, 'UME' as brand from `ume_shopee_shop_dwh.shopee_return_ume_viet_nam`
