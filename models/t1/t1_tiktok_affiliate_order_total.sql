SELECT *,
       'One5' AS company,
       'Chaching' AS brand,
       'chaching_brand' AS shop,
       'VNLCQUWAWR' AS shop_id
FROM `chaching_tiktok_shop_dwh.tiktok_shop_chaching_brand_seller_affiliate_order`

UNION ALL
SELECT *,
       'One5' AS company,
       'Chaching' AS brand,
       'cha_ching_fashion' AS shop,
       'VNLC7YLW9G' AS shop_id
FROM `chaching_2_tiktok_shop_dwh.tiktok_shop_cha_ching_fashion_seller_affiliate_order`

UNION ALL
SELECT *,
       'One5' AS company,
       'Chaching Beauty' AS brand,
       'cha_ching_beauty' AS shop,
       'VNLC9ELWHC' AS shop_id
FROM `chaching_beauty_tiktok_shop_dwh.tiktok_shop_cha_ching_beauty_seller_affiliate_order`

UNION ALL
SELECT *,
       'One5' AS company,
       'LYB' AS brand,
       'lyb_brand' AS shop,
       'VNLCXTWWYS' AS shop_id
FROM `lyb_tiktok_shop_dwh.tiktok_shop_lyb_brand_seller_affiliate_order`

-- UNION ALL
-- SELECT *,
--        'One5' AS company,
--        'LYB Cosmetics' AS brand,
--        'lyb_cosmetic' AS shop,
--        'SHOP005' AS shop_id
-- FROM `lybcosmetic_tiktok_shop_dwh.tiktok_shop_lyb_cosmetic_seller_affiliate_order`

UNION ALL
SELECT *,
       'One5' AS company,
       'UME' AS brand,
       'ume_viet_nam' AS shop,
       'VNLC67WVJH' AS shop_id
FROM `ume_tiktok_shop_dwh.tiktok_shop_ume_viet_nam_seller_affiliate_order`

UNION ALL
SELECT *,
       'One5' AS company,
       'An Cung' AS brand,
       'an_cung_brand' AS shop,
       'VNLCRGWEMH' AS shop_id
FROM `ancung_tiktok_shop_dwh.tiktok_shop_an_cung_brand_seller_affiliate_order`
union all
select *, 'Max Eagle' as company,'BE20' as brand,'be20_viet_nam_store' as shop, 'be20_viet_nam_store' as shop_id from `be20vietnamstore_tiktok_shop_dwh.tiktok_shop_be20_viet_nam_store_seller_affiliate_order`
union all
select *, 'Max Eagle' as company,'UME' as brand,'be20_viet_nam_store' as shop, 'be20_viet_nam_store' as shop_id from `umebeautyvietnam_tiktok_shop_dwh.tiktok_shop_ume_beauty_vietnam_seller_affiliate_order`
union all
select *, 'Max Eagle' as company,'UME' as brand,'the_ume_lab' as shop, 'the_ume_lab' as shop_id from `umebeautynaturalvn_tiktok_shop_dwh.tiktok_shop_the_ume_lab_seller_affiliate_order`
union all
select *, 'Max Eagle' as company,'UME' as brand,'belle_store_vn' as shop, 'belle_store_vn' as shop_id from `bellestorevn_tiktok_shop_dwh.tiktok_shop_belle_store_vn_seller_affiliate_order`
union all
select *, 'Max Eagle' as company,'BE20' as brand,'be20_viet_nam' as shop, 'be20_viet_nam' as shop_id from `be20vietnam3_tiktok_shop_dwh.tiktok_shop_be20_viet_nam_seller_affiliate_order`
union all
select *, 'Max Eagle' as company,'LYB Cosmetics' as brand,'lyb_cosmetic' as shop, 'lyb_cosmetic' as shop_id from `lybcosmeticvn_tiktok_shop_dwh.tiktok_shop_lyb_cosmetic_seller_affiliate_order`
union all
select *, 'Max Eagle' as company,'LYB Cosmetics' as brand,'lyb_beauty' as shop, 'lyb_beauty' as shop_id from `lybbeauty_tiktok_shop_dwh.tiktok_shop_lyb_beauty_seller_affiliate_order`