SELECT 
    ads.*,
    gmv.doanhThuAds as doanhThuShopeeSearch
FROM {{ref("t5_ads_ladi_tiktokGmvDoanhThu_total_with_tkqc")}} AS ads 
LEFT JOIN {{ref("t1_shopee_search_ads_total")}} AS shopeeSearch 
    ON shopeeSearch.date_start = ads.date_start 
    AND cast(shopeeSearch.account_id as string) = ads.idtkqc