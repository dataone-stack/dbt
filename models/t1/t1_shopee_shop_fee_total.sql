SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company, '8750459938-888' AS tax_code, 'Chaching' AS brand, 'Chaching Brand' AS shop,'1360134442' as shop_id
FROM `chaching_shopee_shop_dwh.shopee_payment_escrow_detail_chaching_brand`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company,  '0317423004' AS tax_code, 'LYB' AS brand, 'LYB Official' AS shop, '847813660' as shop_id
FROM `lyb_shopee_shop_dwh.shopee_payment_escrow_detail_lyb_official`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company,  '0317468051' AS tax_code, 'An Cung' AS brand, 'An Cung Brand' AS shop,'1442362266' as shop_id
FROM `ancung_shopee_shop_dwh.shopee_payment_escrow_detail_ancung_brand`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'Max Eagle' as company,  '0317423004' AS tax_code, 'LYB Cosmetics' AS brand, 'LYB Cosmetics' AS shop,'lyb_cosmetic_sp' as shop_id
FROM `lybcosmetic_shopee_shop_dwh.shopee_payment_escrow_detail_lyb_cosmetic`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company,  '0315027453' AS tax_code, 'UME' AS brand, 'UME Viet Nam' AS shop,'1023530981' as shop_id
FROM `ume_shopee_shop_dwh.shopee_payment_escrow_detail_ume_viet_nam`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company,  '-' AS tax_code, 'Omazo' AS brand, 'omazo_vietnam' AS shop, 'omazo_vietnam' shop_id
FROM `omazo_shopee_shop_dwh.shopee_payment_escrow_detail_omazo_vietnam`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company,  '-' AS tax_code, 'Dr Diva' AS brand, 'dr_diva_viet_nam' AS shop, 'dr_diva_viet_nam' as shop_id
FROM `drdiva_shopee_shop_dwh.shopee_payment_escrow_detail_dr_diva_viet_nam`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company,  '-' AS tax_code, 'Chaching Beauty' AS brand, 'Chaching Beauty' AS shop,'1569014047' as shop_id
FROM `chaching_beauty_shopee_shop_dwh.shopee_payment_escrow_detail_chaching_beauty` 
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'One5' as company,  '-' AS tax_code, 'LYB' AS brand, 'LYB Fashion' AS shop, '1648366043' as shop_id
FROM `lybfashion_shopee_shop_dwh.shopee_payment_escrow_detail_lyb_fashion` 









UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'Max Eagle' as company,  '-' AS tax_code, 'BE20' AS brand, 'BE20 kẹo ngậm trắng da' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_payment_escrow_detail_be20_keo_ngam_trang_da`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'Max Eagle' as company,  '-' AS tax_code, 'Cà Phê Mâm Xôi' AS brand, 'Cà phê mâm xôi chính hãng' AS shop, '1333711265' as shop_id
FROM `be20keongamdautamtrangda_shopee_shop_dwh.shopee_payment_escrow_detail_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'UME' AS brand, 'belle_beauty_vietnam_sp' AS shop,'belle_beauty_vietnam_sp' as shop_id
FROM `bellebeautyvietnam_shopee_shop_dwh.shopee_payment_escrow_detail_belle_beauty_vietnam`
-- UNION ALL
-- SELECT order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'Belle Beauty' AS brand, 'belle_vietnam_store' AS shop
-- FROM `bellevietnamstore_shopee_shop_dwh.shopee_payment_escrow_detail_belle_vietnam_store`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'Cà phê gừng' AS brand, 'ca_phe_giam_can_store' AS shop,'ca_phe_giam_can_store' as shop_id
FROM `caphegiamcanstore_shopee_shop_dwh.shopee_payment_escrow_detail_ca_phe_giam_can_store`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'Cà phê gừng' AS brand, 'ca_phe_gung_mat_ong_giam_can' AS shop,'ca_phe_gung_mat_ong_giam_can' as shop_id
FROM `caphegungmatonggiamcan_shopee_shop_dwh.shopee_payment_escrow_detail_ca_phe_gung_mat_ong_giam_can`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'Cà phê gừng' AS brand, 'tong_cong_ty_ca_phe_giam_can' AS shop,'tong_cong_ty_ca_phe_giam_can' as shop_id
FROM `tongcongtycaphegiamcan_shopee_shop_dwh.shopee_payment_escrow_detail_tong_cong_ty_ca_phe_giam_can`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'Cà Phê Mâm Xôi' AS brand, 'cafe_mam_xoi_viet_nam' AS shop, 'cafe_mam_xoi_viet_nam' as shop_id
FROM `caphemamxoivietnam_shopee_shop_dwh.shopee_payment_escrow_detail_cafe_mam_xoi_viet_nam`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' as shop_id
FROM `caphemamxoichinhhang_shopee_shop_dwh.shopee_payment_escrow_detail_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'BE20' AS brand, 'keo_ngam_trang_da_dau_tam_be20' AS shop, 'keo_ngam_trang_da_dau_tam_be20' as shop_id
FROM `keongamtrangdadautambe20_shopee_shop_dwh.shopee_payment_escrow_detail_keo_ngam_trang_da_dau_tam_be20`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'Cà Phê Mâm Xôi' AS brand, 'raspberry_coffee_offlclal' AS shop, 'raspberry_coffee_offlclal' as shop_id
FROM `raspberrycoffeeofficial_shopee_shop_dwh.shopee_payment_escrow_detail_raspberry_coffee_offlclal`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount, 'Max Eagle' as company, '-' AS tax_code, 'UME' AS brand, 'nhat_dang_nhi_da_store' AS shop, 'nhat_dang_nhi_da_store' as shop_id
FROM `nhatdangnhidastore_shopee_shop_dwh.shopee_payment_escrow_detail_nhat_dang_nhi_da_store`
UNION ALL
SELECT 0 as withholding_pit_tax, 0 as withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, seller_voucher_code, seller_shipping_discount,'Max Eagle' as company,  '-' AS tax_code, 'Cà Phê Mâm Xôi' AS brand, 'ca_phe_mam_xoi_chinh_hang' AS shop, 'ca_phe_mam_xoi_chinh_hang' as shop_id
FROM `caphemamxoichinhhang2_shopee_shop_dwh.shopee_payment_escrow_detail_ca_phe_mam_xoi_chinh_hang`
UNION ALL
SELECT 
  0 as withholding_pit_tax, 
  0 as withholding_vat_tax, 
  reverse_shipping_fee, 
  final_return_to_seller_shipping_fee, 
  rsf_seller_protection_fee_claim_amount, 
  order_id, 
  voucher_from_seller, 
  buyer_paid_shipping_fee, 
  buyer_user_name, 
  items, 
  commission_fee, 
  service_fee, 
  seller_transaction_fee, 
  actual_shipping_fee, 
  shopee_shipping_rebate, 
  credit_card_promotion, 
  order_ams_commission_fee, 
  instalment_plan, 
  seller_voucher_code, 
  seller_shipping_discount,
  'Max Eagle' as company,  
  '-' AS tax_code, 
  'Dr Diva' AS brand, 
  'dr_diva_beauty_store' AS shop, 
  'dr_diva_beauty_store' as shop_id
FROM `drdivabeautystore_shopee_shop_dwh.shopee_payment_escrow_detail_dr_diva_beauty_store`

UNION ALL

SELECT 
  withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, 
  rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, 
  buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, 
  shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, 
  seller_voucher_code, seller_shipping_discount,
  'Max Eagle' as company, '-' AS tax_code, 
  'UME' AS brand, 'the_ume_lab' AS shop, 
  'the_ume_lab' as shop_id
FROM `theumelab_shopee_shop_dwh.shopee_payment_escrow_detail_the_ume_lab`

UNION ALL

SELECT 
  withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, 
  rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, 
  buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, 
  shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, 
  seller_voucher_code, seller_shipping_discount,
  'Max Eagle' as company, '-' AS tax_code, 
  'BE20' AS brand, 'be20_viet_nam_store_sp' AS shop, 
  'be20_viet_nam_store_sp' as shop_id
FROM `be20vietnamstore_shopee_shop_dwh.shopee_payment_escrow_detail_be20_viet_nam_store`

UNION ALL

SELECT 
  withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, 
  rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, 
  buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, 
  shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, 
  seller_voucher_code, seller_shipping_discount,
  'Max Eagle' as company, '-' AS tax_code, 
  'UME' AS brand, 'ume_beauty_vietnam_sp' AS shop, 
  'ume_beauty_vietnam_sp' as shop_id
FROM `umebeautyvietnam_shope_shop_dwh.shopee_payment_escrow_detail_ume_beauty_vietnam`

UNION ALL

SELECT 
  withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, 
  rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, 
  buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, 
  shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, 
  seller_voucher_code, seller_shipping_discount,
  'Max Eagle' as company, '-' AS tax_code, 
  'BE20' AS brand, 'Beana beauty VietNam' AS shop, 
  '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_payment_escrow_detail_beana_beauty_vietnam`

UNION ALL

SELECT 
  withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, 
  rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, 
  buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, 
  shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, 
  seller_voucher_code, seller_shipping_discount,
  'Max Eagle' as company, '-' AS tax_code, 
  'Cà Phê Mâm Xôi' AS brand, 'Cà phê mâm xôi store' AS shop, 
  '216095138' as shop_id
FROM `beanabeautyvietnam_shopee_shop_dwh.shopee_payment_escrow_detail_ca_phe_mam_xoi_store`

UNION ALL

SELECT 
  withholding_pit_tax, withholding_vat_tax, reverse_shipping_fee, final_return_to_seller_shipping_fee, 
  rsf_seller_protection_fee_claim_amount, order_id, voucher_from_seller, buyer_paid_shipping_fee, 
  buyer_user_name, items, commission_fee, service_fee, seller_transaction_fee, actual_shipping_fee, 
  shopee_shipping_rebate, credit_card_promotion, order_ams_commission_fee, instalment_plan, 
  seller_voucher_code, seller_shipping_discount,
  'Max Eagle' as company, '-' AS tax_code, 
  'An Cung' AS brand, 'las_beauty' AS shop, 
  'las_beauty' as shop_id
FROM `ancunghaircare_shopee_shop_dwh.shopee_payment_escrow_detail_las_beauty`
