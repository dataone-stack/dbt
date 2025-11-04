SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,'One5' as company, 'Chaching' AS brand, 'Chaching Brand' AS shop, '1360134442' as shop_id
FROM `chaching_shopee_shop_dwh.shopee_return_chaching_brand`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'LYB' AS brand, 'LYB Official' AS shop, '847813660' as shop_id
FROM `lyb_shopee_shop_dwh.shopee_return_lyb_official`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'An Cung' AS brand, 'An Cung Brand' AS shop, '1442362266' as shop_id
FROM `ancung_shopee_shop_dwh.shopee_return_ancung_brand`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'LYB Cosmetics' AS brand, 'lyb_cosmetic_sp' AS shop
FROM `lybcosmetic_shopee_shop_dwh.shopee_return_lyb_cosmetic`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'UME' AS brand, 'ume_viet_nam' AS shop
FROM `ume_shopee_shop_dwh.shopee_return_ume_viet_nam`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'Omazo' AS brand, 'omazo_vietnam' AS shop
FROM `omazo_shopee_shop_dwh.shopee_return_omazo_vietnam`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'Chaching Beauty' AS brand, 'chaching_beauty' AS shop
FROM `chaching_beauty_shopee_shop_dwh.shopee_return_chaching_beauty`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'BE20' AS brand, 'be20_keo_ngam_dau_tam_trang_da' AS shop
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_return_be20_keo_ngam_dau_tam_trang_da`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_return_ca_phe_giam_can_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_return_ca_phe_gung_mat_ong_giam_can`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'raspberry_coffee_offlclal' AS shop
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_return_raspberry_coffee_offlclal`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_return_nhat_dang_nhi_da_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'BE20' AS brand, 'be20_viet_nam_store_sp' AS shop
FROM `be20vietnamstore_shopee_shop_dwh.shopee_return_be20_viet_nam_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'BE20' AS brand, 'beana_beauty_vietnam_sp' AS shop
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_return_beana_beauty_vietnam` where date(datetime_add(update_time, INTERVAL 7 hour)) <= '2025-10-20'
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_store_sp' AS shop
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_store` where date(datetime_add(update_time, INTERVAL 7 hour)) >= '2025-10-21'
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'UME' AS brand, 'belle_beauty_vietnam_sp' AS shop
FROM `bellebeautyvietnam_shopee_shop_dwh.shopee_return_belle_beauty_vietnam`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'An Cung' AS brand, 'las_beauty' AS shop
FROM `ancunghaircare_shopee_shop_dwh.shopee_return_las_beauty`