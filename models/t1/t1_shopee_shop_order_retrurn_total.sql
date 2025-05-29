
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'Chaching' as brand from `chaching_shopee_shop_dwh.shopee_return_chaching_brand`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'LYB' as brand from `lyb_shopee_shop_dwh.shopee_return_lyb_official`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'An Cung' as brand from `ancung_shopee_shop_dwh.shopee_return_ancung_brand`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'LYB Cosmetics' as brand from `lybcosmetic_shopee_shop_dwh.shopee_return_lyb_cosmetic`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'UME' as brand from `ume_shopee_shop_dwh.shopee_return_ume_viet_nam`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'BE20(kẹo ngậm)' as brand from `be20keongamdautamtrangda_shopee_shop_dwh.shopee_return_be20_keo_ngam_dau_tam_trang_da`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'Cà phê gừng' as brand from `caphegiamcanstore_shopee_shop_dwh.shopee_return_ca_phe_giam_can_store`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'Cà phê gừng' as brand from `caphegungmatonggiamcan_shopee_shop_dwh.shopee_return_ca_phe_gung_mat_ong_giam_can`
union all
select order_id,return_seller_due_date,return_id ,update_time,status, item, amount_before_discount, 'Cà phê mâm xôi' as brand from `caphemamxoichinhhang_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`
