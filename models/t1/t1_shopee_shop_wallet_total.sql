SELECT *,'One5' as company, 'Chaching' AS brand, 'Chaching Brand' AS shop, '1360134442' as shop_id
FROM `chaching_shopee_shop_dwh.shopee_payment_wallet_transaction_chaching_brand`
UNION ALL
SELECT *, 'One5' as company, 'LYB' AS brand, 'LYB Official' AS shop, '847813660' as shop_id
FROM `lyb_shopee_shop_dwh.shopee_payment_wallet_transaction_lyb_official`
UNION ALL
SELECT *, 'Max Eagle' as company, 'LYB Cosmetics' AS brand, 'lyb_cosmetic_sp' AS shop, 'lyb_cosmetic_sp' as shop_id
FROM `lybcosmetic_shopee_shop_dwh.shopee_payment_wallet_transaction_lyb_cosmetic`
UNION ALL
SELECT *, 'One5' as company, 'UME' AS brand, 'UME Viet Nam' AS shop, '1023530981' as shop_id
FROM `ume_shopee_shop_dwh.shopee_payment_wallet_transaction_ume_viet_nam`
UNION ALL
SELECT *, 'One5' as company, 'An Cung' AS brand, 'An Cung Brand' AS shop, '1442362266' as shop_id
FROM `ancung_shopee_shop_dwh.shopee_payment_wallet_transaction_ancung_brand`
UNION ALL
SELECT *, 'One5' as company, 'Omazo' AS brand, 'omazo_vietnam' AS shop, 'omazo_vietnam' as shop_id
FROM `omazo_shopee_shop_dwh.shopee_payment_wallet_transaction_omazo_vietnam`
UNION ALL
SELECT *, 'One5' as company, 'Chaching Beauty' AS brand, 'chaching_beauty' AS shop, '1569014047' as shop_id
FROM `chaching_beauty_shopee_shop_dwh.shopee_payment_wallet_transaction_chaching_beauty`
UNION ALL
SELECT *, 'Max Eagle' as company, 'BE20' AS brand, 'BE20 kẹo ngậm trắng da' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_payment_wallet_transaction_be20_keo_ngam_trang_da`
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) <= '2025-10-20'
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'Cà phê mâm xôi chính hãng' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_payment_wallet_transaction_ca_phe_mam_xoi_chinh_hang`
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) >= '2025-10-21'
UNION ALL
SELECT *, 'Max Eagle' as company, 'UME' AS brand, 'belle_beauty_vietnam_sp' AS shop, 'belle_beauty_vietnam_sp' AS shop_id
FROM `bellebeautyvietnam_shopee_shop_dwh.shopee_payment_wallet_transaction_belle_beauty_vietnam`
-- UNION ALL
-- SELECT *, 'Max Eagle' as company, 'Belle Beauty' AS brand, 'belle_vietnam_store' AS shop, 'belle_vietnam_store' AS shop_id
-- FROM `bellevietnamstore_shopee_shop_dwh.shopee_payment_wallet_transaction_belle_vietnam_store`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'tong_cong_ty_ca_phe_giam_can' AS shop, 'tong_cong_ty_ca_phe_giam_can' AS shop_id
FROM `tongcongtycaphegiamcan_shopee_shop_dwh.shopee_payment_wallet_transaction_tong_cong_ty_ca_phe_giam_can`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop, 'ca_phe_gung_mat_ong_giam_can' AS shop_id
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_payment_wallet_transaction_ca_phe_gung_mat_ong_giam_can`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop, 'ca_phe_giam_can_store' AS shop_id
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_payment_wallet_transaction_ca_phe_giam_can_store`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' AS shop_id
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_payment_wallet_transaction_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'cafe_mam_xoi_viet_nam' AS shop, 'cafe_mam_xoi_viet_nam' AS shop_id
FROM `caphemamxoivietnam_shopee_shop_dwh.shopee_payment_wallet_transaction_cafe_mam_xoi_viet_nam`
UNION ALL
SELECT *, 'Max Eagle' as company, 'BE20' AS brand, 'keo_ngam_trang_da_dau_tam_be20' AS shop, 'keo_ngam_trang_da_dau_tam_be20' AS shop_id
FROM `keongamtrangdadautambe20_shopee_shop_dwh.shopee_payment_wallet_transaction_keo_ngam_trang_da_dau_tam_be20`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'raspberry_coffee_offlclal' AS shop, 'raspberry_coffee_offlclal' AS shop_id
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_payment_wallet_transaction_raspberry_coffee_offlclal`
UNION ALL
SELECT *, 'Max Eagle' as company, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop, 'nhat_dang_nhi_da_store' AS shop_id
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_payment_wallet_transaction_nhat_dang_nhi_da_store`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' AS shop_id
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_payment_wallet_transaction_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT *, 'Max Eagle' as company, 'Dr Diva' AS brand, 'dr_diva_beauty_store' AS shop, 'dr_diva_beauty_store' AS shop_id
FROM `drdivabeautystore_shopee_shop_dwh.shopee_payment_wallet_transaction_dr_diva_beauty_store`
UNION ALL
SELECT *, 'Max Eagle' as company, 'UME' AS brand, 'the_ume_lab' AS shop, 'the_ume_lab' AS shop_id
FROM `theumelab_shopee_shop_dwh.shopee_payment_wallet_transaction_the_ume_lab`
UNION ALL
SELECT *, 'Max Eagle' as company, 'BE20' AS brand, 'be20_viet_nam_store_sp' AS shop, 'be20_viet_nam_store_sp' AS shop_id
FROM `be20vietnamstore_shopee_shop_dwh.shopee_payment_wallet_transaction_be20_viet_nam_store`
UNION ALL
SELECT *, 'Max Eagle' as company, 'UME' AS brand, 'ume_beauty_vietnam_sp' AS shop, 'ume_beauty_vietnam_sp' AS shop_id
FROM `umebeautyvietnam_shope_shop_dwh.shopee_payment_wallet_transaction_ume_beauty_vietnam`
UNION ALL
SELECT *, 'Max Eagle' as company, 'BE20' AS brand, 'Beana beauty VietNam' AS shop, '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_payment_wallet_transaction_beana_beauty_vietnam`
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) <= '2025-10-20'
UNION ALL
SELECT *, 'Max Eagle' as company, 'Cà Phê Mâm Xôi' AS brand, 'Cà phê mâm xôi store' AS shop, '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_payment_wallet_transaction_ca_phe_mam_xoi_store`
WHERE DATE(DATETIME_ADD(create_time, INTERVAL 7 HOUR)) >= '2025-10-21'
UNION ALL
SELECT *, 'Max Eagle' as company, 'An Cung' AS brand, 'las_beauty' AS shop, 'las_beauty' AS shop_id
FROM `ancunghaircare_shopee_shop_dwh.shopee_payment_wallet_transaction_las_beauty`
