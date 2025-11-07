SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Chaching' AS brand, 'Chaching Brand' AS shop, '1360134442' as shop_id
FROM `chaching_shopee_shop_dwh.shopee_order_detail_chaching_brand`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'LYB' AS brand, 'LYB Official' AS shop , '847813660' as shop_id
FROM `lyb_shopee_shop_dwh.shopee_order_detail_lyb_official`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'An Cung' AS brand, 'An Cung Brand' AS shop, '1442362266' as shop_id
FROM `ancung_shopee_shop_dwh.shopee_order_detail_ancung_brand`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'LYB Cosmetics' AS brand, 'LYB Cosmetics' AS shop, '1266788038' as shop_id
FROM `lybcosmetic_shopee_shop_dwh.shopee_order_detail_lyb_cosmetic`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'UME' AS brand, 'UME Viet Nam' AS shop, '1023530981' as shop_id
FROM `ume_shopee_shop_dwh.shopee_order_detail_ume_viet_nam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Omazo' AS brand, 'omazo_vietnam' AS shop, 'omazo_vietnam' as shop_id
FROM `omazo_shopee_shop_dwh.shopee_order_detail_omazo_vietnam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Dr Diva' AS brand, 'dr_diva_viet_nam' AS shop, 'dr_diva_viet_nam' as shop_id
FROM `drdiva_shopee_shop_dwh.shopee_order_detail_dr_diva_viet_nam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Chaching Beauty' AS brand, 'Chaching Beauty' AS shop, '1569014047' as shop_id
FROM `chaching_beauty_shopee_shop_dwh.shopee_order_detail_chaching_beauty`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'LYB' AS brand, 'LYB Fashion' AS shop, '1648366043' as shop_id
FROM `lybfashion_shopee_shop_dwh.shopee_order_detail_lyb_fashion`





UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'BE20' AS brand, 'BE20 kẹo ngậm trắng da' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_order_detail_be20_keo_ngam_trang_da` 
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) <= '2025-10-20'

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'Cà phê mâm xôi chính hãng' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_chinh_hang`  
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) >= '2025-10-21'

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'UME' AS brand, 'belle_beauty_vietnam_sp' AS shop, 'belle_beauty_vietnam_sp' as shop_id
FROM `bellebeautyvietnam_shopee_shop_dwh.shopee_order_detail_belle_beauty_vietnam`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà phê gừng' AS brand, 'tong_cong_ty_ca_phe_giam_can' AS shop, 'tong_cong_ty_ca_phe_giam_can' as shop_id
FROM `tongcongtycaphegiamcan_shopee_shop_dwh.shopee_order_detail_tong_cong_ty_ca_phe_giam_can`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop, 'ca_phe_gung_mat_ong_giam_can' as shop_id
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_order_detail_ca_phe_gung_mat_ong_giam_can`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop, 'ca_phe_giam_can_store' as shop_id
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_order_detail_ca_phe_giam_can_store`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' as shop_id
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_chinh_hang`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'cafe_mam_xoi_viet_nam' AS shop, 'cafe_mam_xoi_viet_nam' as shop_id
FROM `caphemamxoivietnam_shopee_shop_dwh.shopee_order_detail_cafe_mam_xoi_viet_nam`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'BE20' AS brand, 'keo_ngam_trang_da_dau_tam_be20' AS shop, 'keo_ngam_trang_da_dau_tam_be20' as shop_id
FROM `keongamtrangdadautambe20_shopee_shop_dwh.shopee_order_detail_keo_ngam_trang_da_dau_tam_be20`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'raspberry_coffee_offlclal' AS shop, 'raspberry_coffee_offlclal' as shop_id
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_order_detail_raspberry_coffee_offlclal`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop, 'nhat_dang_nhi_da_store' as shop_id
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_order_detail_nhat_dang_nhi_da_store`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' as shop_id
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_chinh_hang`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Dr Diva' AS brand, 'dr_diva_beauty_store' AS shop, 'dr_diva_beauty_store' as shop_id
FROM `drdivabeautystore_shopee_shop_dwh.shopee_order_detail_dr_diva_beauty_store`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'UME' AS brand, 'the_ume_lab' AS shop, 'the_ume_lab' as shop_id
FROM `theumelab_shopee_shop_dwh.shopee_order_detail_the_ume_lab`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'BE20' AS brand, 'BE20 Việt Nam Store' AS shop, '1414505993' as shop_id
FROM `be20vietnamstore_shopee_shop_dwh.shopee_order_detail_be20_viet_nam_store`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'UME' AS brand, 'UME Beauty VN' AS shop, '1601895285' as shop_id
FROM `umebeautyvietnam_shope_shop_dwh.shopee_order_detail_ume_beauty_vietnam`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'BE20' AS brand, 'Beana beauty VietNam' AS shop, '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_order_detail_beana_beauty_vietnam` 
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) <= '2025-10-20'

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'Cà phê mâm xôi store' AS shop, '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_store` 
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) >= '2025-10-21'

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Dr Diva' AS brand, 'Las Beauty' AS shop, '1506821514' as shop_id
FROM `ancunghaircare_shopee_shop_dwh.shopee_order_detail_las_beauty`

UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 
'Max Eagle' as company, 'Chanh tây' AS brand, 'Lemon Coffee' AS shop, '1662596872' as shop_id
FROM `lemoncoffee_shopee_shop_dwh.shopee_order_detail_ca_phe_chanh_tay_lemon_coffee`
