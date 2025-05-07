SELECT 
    ads.*,
    CASE 
        WHEN gmv.doanhThuAds IS NOT NULL 
        THEN ads.doanhThuAds + COALESCE(cast(gmv.doanhThuAds as int64), 0)
        ELSE ads.doanhThuAds
    END AS doanhThuAds
FROM {{ref("t4_ads_ladipageFacebook_total_with_tkqc")}} AS ads 
LEFT JOIN {{ref("t1_tiktokGMV_ads_doanhThu_total")}} AS gmv 
    ON gmv.date_start = ads.date_start 
    AND gmv.account_id = ads.idtkqc