SELECT 'an_cung' AS brand, *, ARRAY_LENGTH(line_items) AS item_count FROM `ancung_tiktok_shop_dwh.tiktok_shop_an_cung_brand_order`
  UNION ALL 
  SELECT 'chaching' AS brand, *, ARRAY_LENGTH(line_items) AS item_count FROM `chaching_tiktok_shop_dwh.tiktok_shop_chaching_brand_order`
  UNION ALL
  SELECT 'lyb' AS brand, *, ARRAY_LENGTH(line_items) AS item_count FROM `lyb_tiktok_shop_dwh.tiktok_shop_lyb_brand_order`
  UNION ALL
  SELECT 'lybcosmetic' AS brand, *, ARRAY_LENGTH(line_items) AS item_count FROM `lybcosmetic_tiktok_shop_dwh.tiktok_shop_lyb_cosmetic_order`
  UNION ALL
  SELECT 'ume' AS brand, *, ARRAY_LENGTH(line_items) AS item_count FROM `ume_tiktok_shop_dwh.tiktok_shop_ume_viet_nam_order`