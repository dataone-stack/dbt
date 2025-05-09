SELECT
    a.date_start,
    a.brand,
    a.channel,
    (COALESCE(a.doanhThuAds, 0) + COALESCE(a.doanhThuLadi, 0) + COALESCE(a.doanhThuGMVTiktok, 0)) AS totalAds,
    p.total_price_after_sub_discount AS totalDoanhThuPos
FROM {{ ref('t3_ads_total_with_tkqc') }} a
FULL OUTER JOIN {{ ref('t1_pancake_pos_order_total') }} p
    ON a.date_start = DATE(p.inserted_at)
    AND a.brand = p.brand
    AND a.channel = CASE 
                        WHEN p.order_sources_name = 'Ladipage Facebook' THEN 'Facebook'
                        WHEN p.order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
                        ELSE p.order_sources_name 
                    END