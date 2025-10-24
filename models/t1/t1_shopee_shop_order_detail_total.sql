SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Chaching' AS brand, 'chaching_brand' AS shop
FROM `chaching_shopee_shop_dwh.shopee_order_detail_chaching_brand`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'LYB' AS brand, 'lyb_official' AS shop
FROM `lyb_shopee_shop_dwh.shopee_order_detail_lyb_official`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'An Cung' AS brand, 'ancung_brand' AS shop
FROM `ancung_shopee_shop_dwh.shopee_order_detail_ancung_brand`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'LYB Cosmetics' AS brand, 'lyb_cosmetic_sp' AS shop
FROM `lybcosmetic_shopee_shop_dwh.shopee_order_detail_lyb_cosmetic`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'UME' AS brand, 'ume_viet_nam' AS shop
FROM `ume_shopee_shop_dwh.shopee_order_detail_ume_viet_nam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Omazo' AS brand, 'omazo_vietnam' AS shop
FROM `omazo_shopee_shop_dwh.shopee_order_detail_omazo_vietnam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Dr Diva' AS brand, 'dr_diva_viet_nam' AS shop
FROM `drdiva_shopee_shop_dwh.shopee_order_detail_dr_diva_viet_nam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'Chaching Beauty' AS brand, 'chaching_beauty' AS shop
FROM `chaching_beauty_shopee_shop_dwh.shopee_order_detail_chaching_beauty`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'One5' as company, 'LYB' AS brand, 'lyb_fashion' AS shop
FROM `lybfashion_shopee_shop_dwh.shopee_order_detail_lyb_fashion`


















UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'BE20' AS brand, 'be20_keo_ngam_trang_da_sp' AS shop
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_order_detail_be20_keo_ngam_trang_da`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'UME' AS brand, 'belle_beauty_vietnam_sp' AS shop
FROM `bellebeautyvietnam_shopee_shop_dwh.shopee_order_detail_belle_beauty_vietnam`
UNION ALL
-- SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Belle Beauty' AS brand, 'belle_vietnam_store' AS shop
-- FROM `3`
-- UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'tong_cong_ty_ca_phe_giam_can' AS shop
FROM `tongcongtycaphegiamcan_shopee_shop_dwh.shopee_order_detail_tong_cong_ty_ca_phe_giam_can`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_order_detail_ca_phe_gung_mat_ong_giam_can`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_order_detail_ca_phe_giam_can_store`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'cafe_mam_xoi_viet_nam' AS shop
FROM `caphemamxoivietnam_shopee_shop_dwh.shopee_order_detail_cafe_mam_xoi_viet_nam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'BE20' AS brand, 'keo_ngam_trang_da_dau_tam_be20' AS shop
FROM `keongamtrangdadautambe20_shopee_shop_dwh.shopee_order_detail_keo_ngam_trang_da_dau_tam_be20`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'raspberry_coffee_offlclal' AS shop
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_order_detail_raspberry_coffee_offlclal`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_order_detail_nhat_dang_nhi_da_store`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'Dr Diva' AS brand, 'dr_diva_beauty_store' AS shop
FROM `drdivabeautystore_shopee_shop_dwh.shopee_order_detail_dr_diva_beauty_store`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'UME' AS brand, 'the_ume_lab' AS shop
FROM `theumelab_shopee_shop_dwh.shopee_order_detail_the_ume_lab`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'BE20' AS brand, 'be20_viet_nam_store_sp' AS shop
FROM `be20vietnamstore_shopee_shop_dwh.shopee_order_detail_be20_viet_nam_store`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'UME' AS brand, 'ume_beauty_vietnam_sp' AS shop
FROM `umebeautyvietnam_shope_shop_dwh.shopee_order_detail_ume_beauty_vietnam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'BE20' AS brand, 'beana_beauty_vietnam_sp' AS shop
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_order_detail_beana_beauty_vietnam`
UNION ALL
SELECT item_list, order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Max Eagle' as company, 'An Cung' AS brand, 'las_beauty' AS shop
FROM `ancunghaircare_shopee_shop_dwh.shopee_order_detail_las_beauty`