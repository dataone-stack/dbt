SELECT 
    DATE(inserted_at) AS date,
    brand,
    CASE 
        WHEN order_sources_name = 'Ladipage Facebook' THEN 'Facebook'
        WHEN order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
        ELSE order_sources_name 
    END AS channel,
    COUNT(*) AS row_count,
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
ORDER BY date, brand, channel;