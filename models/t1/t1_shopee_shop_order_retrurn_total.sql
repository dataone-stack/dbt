SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,'One5' as company, 'Chaching' AS brand, 'Chaching Brand' AS shop, '1360134442' as shop_id
FROM `chaching_shopee_shop_dwh.shopee_return_chaching_brand`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'LYB' AS brand, 'LYB Official' AS shop, '847813660' as shop_id
FROM `lyb_shopee_shop_dwh.shopee_return_lyb_official`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'An Cung' AS brand, 'An Cung Brand' AS shop, '1442362266' as shop_id
FROM `ancung_shopee_shop_dwh.shopee_return_ancung_brand`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'Max Eagle' as company, 'LYB Cosmetics' AS brand, 'LYB Cosmetics' AS shop, '1266788038' as shop_id
FROM `lybcosmetic_shopee_shop_dwh.shopee_return_lyb_cosmetic`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'UME' AS brand, 'UME Viet Nam' AS shop,'1023530981' as shop_id
FROM `ume_shopee_shop_dwh.shopee_return_ume_viet_nam`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'Omazo' AS brand, 'omazo_vietnam' AS shop, 'omazo_vietnam' as shop_id
FROM `omazo_shopee_shop_dwh.shopee_return_omazo_vietnam`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount, 'One5' as company, 'Chaching Beauty' AS brand, 'Chaching Beauty' AS shop, '1569014047' as shop_id
FROM `chaching_beauty_shopee_shop_dwh.shopee_return_chaching_beauty`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'BE20' AS brand, 'be20_keo_ngam_dau_tam_trang_da' AS shop, 'be20_keo_ngam_dau_tam_trang_da' AS shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_return_be20_keo_ngam_dau_tam_trang_da`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'BE20' AS brand, 'BE20 kẹo ngậm trắng da' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_return_be20_keo_ngam_trang_da`
union all
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'BE20' AS brand, 'Cà phê mâm xôi chính hãng' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`


union all
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop, 'ca_phe_giam_can_store' AS shop_id
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_return_ca_phe_giam_can_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop, 'ca_phe_gung_mat_ong_giam_can' AS shop_id
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_return_ca_phe_gung_mat_ong_giam_can`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' AS shop_id
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'raspberry_coffee_offlclal' AS shop, 'raspberry_coffee_offlclal' AS shop_id
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_return_raspberry_coffee_offlclal`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop, 'nhat_dang_nhi_da_store' AS shop_id
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_return_nhat_dang_nhi_da_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' AS shop_id
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'BE20' AS brand, 'BE20 Việt Nam Store' AS shop, '1414505993' AS shop_id
FROM `be20vietnamstore_shopee_shop_dwh.shopee_return_be20_viet_nam_store`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'BE20' AS brand, 'Beana beauty VietNam' AS shop, '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_return_beana_beauty_vietnam`
WHERE DATE(DATETIME_ADD(update_time, INTERVAL 7 HOUR)) <= '2025-10-20'
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'Cà phê mâm xôi store' AS shop, '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_return_ca_phe_mam_xoi_store`
WHERE DATE(DATETIME_ADD(update_time, INTERVAL 7 HOUR)) >= '2025-10-21'
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'UME' AS brand, 'belle_beauty_vietnam_sp' AS shop, 'belle_beauty_vietnam_sp' AS shop_id
FROM `bellebeautyvietnam_shopee_shop_dwh.shopee_return_belle_beauty_vietnam`
UNION ALL
SELECT order_id, return_seller_due_date, return_id, update_time, status, item, amount_before_discount,
       'Max Eagle' as company, 'Dr Diva' AS brand, 'La Beauty' AS shop, '1506821514' AS shop_id
FROM `ancunghaircare_shopee_shop_dwh.shopee_return_las_beauty`
