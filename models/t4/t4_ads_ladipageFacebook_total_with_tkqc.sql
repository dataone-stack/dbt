WITH ladi_aggregated AS (
    SELECT 
        date_insert,
        id_staff,
        manager,
        brand,
        channel,
        SUM(doanhThuLadi) AS doanhThuLadi
    FROM {{ ref("t2_ladipage_facebook_total") }}
    GROUP BY date_insert, id_staff, manager, brand, channel
)
SELECT 
    ads.*,
    COALESCE(ladi_aggregated.doanhThuLadi, 0) AS doanhThuLadi
FROM {{ ref("t3_ads_total_with_tkqc") }} AS ads
LEFT JOIN ladi_aggregated
    ON ads.date_start = ladi_aggregated.date_insert 
    AND ads.ma_nhan_vien = ladi_aggregated.id_staff 
    AND ads.manager = ladi_aggregated.manager 
    AND ads.brand = ladi_aggregated.brand 
    AND ads.channel = ladi_aggregated.channel