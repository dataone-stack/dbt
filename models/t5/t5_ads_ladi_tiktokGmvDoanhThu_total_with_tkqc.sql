SELECT
    ads.*,
    gmv.doanhThuAds as doanhThuGMVTiktok
FROM {{ref("t4_ads_ladipageFacebook_total_with_tkqc")}} AS ads 
LEFT JOIN {{ref("t1_tiktokGMV_ads_doanhThu_total")}} AS gmv 
    ON gmv.date_start = ads.date_start 
    AND cast(gmv.account_id as string) = ads.idtkqc