SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Chaching' AS brand, 'chaching_brand' AS shop
FROM `chaching_shopee_shop_dwh.shopee_order_detail_chaching_brand`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'LYB' AS brand, 'lyb_official' AS shop
FROM `lyb_shopee_shop_dwh.shopee_order_detail_lyb_official`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'An Cung' AS brand, 'ancung_brand' AS shop
FROM `ancung_shopee_shop_dwh.shopee_order_detail_ancung_brand`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'LYB Cosmetics' AS brand, 'lyb_cosmetic' AS shop
FROM `lybcosmetic_shopee_shop_dwh.shopee_order_detail_lyb_cosmetic`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'UME' AS brand, 'ume_viet_nam' AS shop
FROM `ume_shopee_shop_dwh.shopee_order_detail_ume_viet_nam`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'BE20(kẹo ngậm)' AS brand, 'be20_keo_ngam_dau_tam_trang_da' AS shop
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_order_detail_be20_keo_ngam_dau_tam_trang_da`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Belle Beauty' AS brand, 'belle_beauty_vietnam' AS shop
FROM `bellebeautyvietnam_shopee_shop_dwh.shopee_order_detail_belle_beauty_vietnam`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Belle Beauty' AS brand, 'belle_vietnam_store' AS shop
FROM `bellevietnamstore_shopee_shop_dwh.shopee_order_detail_belle_vietnam_store`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Cà phê gừng' AS brand, 'tong_cong_ty_ca_phe_giam_can' AS shop
FROM `tongcongtycaphegiamcan_shopee_shop_dwh.shopee_order_detail_tong_cong_ty_ca_phe_giam_can`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_order_detail_ca_phe_gung_mat_ong_giam_can`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_order_detail_ca_phe_giam_can_store`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Cà phê mâm xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Cà phê mâm xôi' AS brand, 'cafe_mam_xoi_viet_nam' AS shop
FROM `caphemamxoivietnam_shopee_shop_dwh.shopee_order_detail_cafe_mam_xoi_viet_nam`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'BE20(kẹo ngậm)' AS brand, 'keo_ngam_trang_da_dau_tam_be20' AS shop
FROM `keongamtrangdadautambe20_shopee_shop_dwh.shopee_order_detail_keo_ngam_trang_da_dau_tam_be20`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Cà phê mâm xôi' AS brand, 'raspberry_coffee_offlclal' AS shop
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_order_detail_raspberry_coffee_offlclal`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_order_detail_nhat_dang_nhi_da_store`
UNION ALL
SELECT order_id, total_amount, create_time, order_status, payment_method, shipping_carrier, ship_by_date, buyer_cancel_reason, days_to_ship, checkout_shipping_carrier, 'Cà phê mâm xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_order_detail_ca_phe_mam_xoi_chinh_hang`