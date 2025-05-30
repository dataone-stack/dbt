SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Chaching' AS brand, 'chaching_brand' AS shop
FROM `chaching_shopee_shop_dwh.shopee_return_chaching_brand`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'LYB' AS brand, 'lyb_official' AS shop
FROM `lyb_shopee_shop_dwh.shopee_return_lyb_official`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'An Cung' AS brand, 'ancung_brand' AS shop
FROM `ancung_shopee_shop_dwh.shopee_return_ancung_brand`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'LYB Cosmetics' AS brand, 'lyb_cosmetic' AS shop
FROM `lybcosmetic_shopee_shop_dwh.shopee_return_lyb_cosmetic`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'UME' AS brand, 'ume_viet_nam' AS shop
FROM `ume_shopee_shop_dwh.shopee_return_ume_viet_nam`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'BE20(kẹo ngậm)' AS brand, 'be20_keo_ngam_dau_tam_trang_da' AS shop
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_return_be20_keo_ngam_dau_tam_trang_da`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_return_ca_phe_giam_can_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_return_ca_phe_gung_mat_ong_giam_can`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Cà phê mâm xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Cà phê mâm xôi' AS brand, 'raspberry_coffee_offlclal' AS shop
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_return_raspberry_coffee_offlclal`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_return_nhat_dang_nhi_da_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Cà phê mâm xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`