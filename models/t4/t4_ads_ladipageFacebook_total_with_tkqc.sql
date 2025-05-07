SELECT 
    ads.*,
    CASE 
        WHEN ROW_NUMBER() OVER (
            PARTITION BY ads.date_start, ads.ma_nhan_vien, ads.manager, ads.brand, ads.channel 
            ORDER BY ladi.date_insert
        ) = 1 THEN ladi.doanhThuLadi 
        ELSE 0 
    END AS doanhThuLadi
FROM {{ref("t3_ads_total_with_tkqc")}} AS ads
LEFT JOIN {{ref("t2_ladipage_facebook_total")}} AS ladi
    ON ads.date_start = ladi.date_insert 
    AND ads.ma_nhan_vien = ladi.id_staff 
    AND ads.manager = ladi.manager 
    AND ads.brand = ladi.brand 
    AND ads.channel = ladi.channel