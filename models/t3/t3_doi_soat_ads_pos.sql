with pos as (
SELECT 
    DATE(inserted_at) AS date,
    brand,
    CASE 
        WHEN order_sources_name = 'Ladipage Facebook' THEN 'Facebook'
        WHEN order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
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
        ELSE order_sources_name 
    END 
ORDER BY date, brand, channel
)

select 
    pos.*,
    sum(COALESCE(ads.doanhThuShopeeSearch, 0) + COALESCE(ads.doanhThuAds, 0) + COALESCE(ads.doanhThuLadi, 0) + COALESCE(ads.doanhThuGMVTiktok, 0)) AS totalAds
from pos as pos full outer join {{ref("t3_ads_total_with_tkqc")}} as ads
on pos.date = ads.date_start and pos.brand = ads.brand and pos.channel = ads.channel
group by pos.date, pos.brand,pos.channel,pos.total_pos_revenue
