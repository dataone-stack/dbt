WITH pos AS (
    SELECT 
        DATE(inserted_at) AS date,
        brand,
        CASE 
            WHEN order_sources_name = 'Ladipage Facebook' THEN 'Facebook'
            WHEN order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
            WHEN order_sources_name = 'Webcake' THEN 'Facebook'
            ELSE order_sources_name 
        END AS channel,
        SUM(total_price_after_sub_discount) AS total_pos_revenue
    FROM {{ ref('t1_pancake_pos_order_total') }}
    GROUP BY 
        DATE(inserted_at),
        brand,
        CASE 
            WHEN order_sources_name = 'Ladipage Facebook' THEN 'Facebook'
            WHEN order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
            WHEN order_sources_name = 'Webcake' THEN 'Facebook'
            ELSE order_sources_name 
        END 
)

SELECT 
    pos.date,
    pos.brand,
    pos.channel,
    pos.total_pos_revenue,
    COALESCE(
        (SELECT STRING_AGG(DISTINCT manager, ', ') 
         FROM {{ ref('t3_ads_total_with_tkqc') }} ads 
         WHERE ads.date_start = pos.date 
           AND ads.brand = pos.brand 
           AND ads.channel = pos.channel),
        ''
    ) AS manager,
    COALESCE(
        (SELECT STRING_AGG(DISTINCT staff, ', ') 
         FROM {{ ref('t3_ads_total_with_tkqc') }} ads 
         WHERE ads.date_start = pos.date 
           AND ads.brand = pos.brand 
           AND ads.channel = pos.channel),
        ''
    ) AS staff,
    COALESCE(
        (SELECT STRING_AGG(DISTINCT nametkqc, ', ') 
         FROM {{ ref('t3_ads_total_with_tkqc') }} ads 
         WHERE ads.date_start = pos.date 
           AND ads.brand = pos.brand 
           AND ads.channel = pos.channel),
        ''
    ) AS nametkqc,
    COALESCE(
        (SELECT SUM(COALESCE(doanhThuShopeeSearch, 0) + COALESCE(doanhThuAds, 0) + COALESCE(doanhThuLadi, 0) + COALESCE(doanhThuGMVTiktok, 0))
         FROM {{ ref('t3_ads_total_with_tkqc') }} ads
         WHERE ads.date_start = pos.date
           AND ads.brand = pos.brand
           AND ads.channel = pos.channel),
        0
    ) AS totalAds
FROM pos
ORDER BY pos.date, pos.brand, pos.channel