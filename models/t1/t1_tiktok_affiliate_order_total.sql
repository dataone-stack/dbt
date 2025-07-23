select *,'One5' as company, 'Chaching' as brand,'chaching_brand' as shop from `chaching_tiktok_shop_dwh.tiktok_shop_chaching_brand_seller_affiliate_order`
union all
select *,'One5' as company, 'Chaching' as brand,'cha_ching_fashion' as shop from `chaching_2_tiktok_shop_dwh.tiktok_shop_cha_ching_fashion_seller_affiliate_order`
union all
select *,'One5' as company,'Chaching Beauty' as brand,'cha_ching_beauty' as shop from `chaching_beauty_tiktok_shop_dwh.tiktok_shop_cha_ching_beauty_seller_affiliate_order`
union all
select *, 'One5' as company,'LYB' as brand,'lyb_brand' as shop from `lyb_tiktok_shop_dwh.tiktok_shop_lyb_brand_seller_affiliate_order`
union all
select *, 'One5' as company,'LYB Cosmetics' as brand,'lyb_cosmetic' as shop from `lybcosmetic_tiktok_shop_dwh.tiktok_shop_lyb_cosmetic_seller_affiliate_order`
union all
select *, 'One5' as company,'UME' as brand,'ume_viet_nam' as shop from `ume_tiktok_shop_dwh.tiktok_shop_ume_viet_nam_seller_affiliate_order`
union all
select *, 'One5' as company,'An Cung' as brand,'an_cung_brand' as shop from `ancung_tiktok_shop_dwh.tiktok_shop_an_cung_brand_seller_affiliate_order`