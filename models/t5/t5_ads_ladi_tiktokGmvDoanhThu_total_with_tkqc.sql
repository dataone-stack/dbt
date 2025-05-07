SELECT 
    -- ads.date_start,
    -- ads.idtkqc,
    -- ads.nametkqc,
    -- ads.ma_nhan_vien,
    -- ads.staff,
    -- ads.manager,
    -- ads.brand,
    -- ads.channel,
    -- ads.chiPhiAds,
    -- ads.doanhThuLadi,
    ads.*,
    gmv.doanhThuAds as doanhThuGMVTiktok
FROM {{ref("t4_ads_ladipageFacebook_total_with_tkqc")}} AS ads 
LEFT JOIN {{ref("t1_tiktokGMV_ads_doanhThu_total")}} AS gmv 
    ON gmv.date_start = ads.date_start 
    AND cast(gmv.account_id as string) = ads.idtkqc