SELECT
    a.date_start,
    a.brand,
    a.channel,
    COALESCE(a.doanhThuShopeeSearch, 0) + COALESCE(a.doanhThuAds, 0) + COALESCE(a.doanhThuLadi, 0) + COALESCE(a.doanhThuGMVTiktok, 0) AS totalAds,
    COALESCE(SUM(p.total_price_after_sub_discount), 0) AS totalDoanhThuPos
FROM {{ ref('t3_ads_total_with_tkqc') }} a
FULL OUTER JOIN (
    SELECT 
        DATE(inserted_at) AS date,
        brand,
        CASE 
            WHEN order_sources_name = 'Ladipage Facebook' THEN 'Facebook'
            WHEN order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
            ELSE order_sources_name 
        END AS channel,
        SUM(total_price_after_sub_discount) AS total_price_after_sub_discount
    FROM {{ ref('t1_pancake_pos_order_total') }}
    GROUP BY 
        DATE(inserted_at),
        brand,
        CASE 
            WHEN order_sources_name = 'Ladipage Facebook' THEN 'Facebook'
            WHEN order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
            ELSE order_sources_name 
        END
) p
    ON a.date_start = p.date
    AND a.brand = p.brand
    AND a.channel = p.channel
GROUP BY 
    a.date_start,
    a.brand,
    a.channel,
    (COALESCE(a.doanhThuShopeeSearch, 0) + COALESCE(a.doanhThuAds, 0) + COALESCE(a.doanhThuLadi, 0) + COALESCE(a.doanhThuGMVTiktok, 0))