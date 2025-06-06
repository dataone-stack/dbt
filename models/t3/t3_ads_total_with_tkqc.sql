WITH ads_total_with_tkqc AS (
SELECT
ads.date_start,
ads.revenue_type,
tkqc.idtkqc,
tkqc.nametkqc,
tkqc.ma_nhan_vien,
tkqc.staff,
tkqc.manager,
tkqc.brand,
tkqc.channel,
SUM(ads.spend) AS chiPhiAds,
SUM(ads.doanhThuAds) AS doanhThuAds
FROM {{ ref('t2_ads_total')}} AS ads
RIGHT JOIN {{ ref('t1_tkqc') }} AS tkqc
ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
GROUP BY
ads.date_start,
tkqc.idtkqc,
tkqc.nametkqc,
tkqc.ma_nhan_vien,
tkqc.staff,
tkqc.manager,
tkqc.brand,
tkqc.channel,
ads.revenue_type
),

ads_ladipageFacebook_total_with_tkqc AS (
SELECT
ads.*,
CASE
WHEN ROW_NUMBER() OVER (
PARTITION BY ads.date_start, ads.ma_nhan_vien, ads.manager, ads.brand, ads.channel
ORDER BY ladi.date_insert
) = 1 THEN ladi.doanhThuLadi
ELSE 0
END AS doanhThuLadi,
FROM ads_total_with_tkqc AS ads
LEFT JOIN {{ ref('t2_ladipage_facebook_total') }} AS ladi
ON ads.date_start = ladi.date_insert
AND ads.ma_nhan_vien = ladi.id_staff
AND ads.manager = ladi.manager
AND ads.brand = ladi.brand
AND ads.channel = ladi.channel
)

SELECT
ads.date_start,
ads.idtkqc,
ads.nametkqc,
ads.ma_nhan_vien,
ads.staff,
ads.manager,
ads.brand,
ads.channel,
ads.chiPhiAds,
ads.doanhThuAds,
ads.doanhThuLadi,
gmv.gross_revenue AS doanhThuGMVTiktok,
shopeeSearch.doanhThuAds AS doanhThuShopeeSearch,
ads.revenue_type AS loaiDoanhThu
FROM ads_ladipageFacebook_total_with_tkqc AS ads
LEFT JOIN {{ ref('t1_tiktokGMV_ads_total') }} AS gmv
ON DATE(DATETIME_ADD(DATETIME(gmv.stat_time_day), INTERVAL 7 HOUR)) = ads.date_start
AND CAST(gmv.account_id AS STRING) = CAST(ads.idtkqc AS STRING)
LEFT JOIN {{ ref('t1_shopee_search_ads_total') }} AS shopeeSearch
ON shopeeSearch.date_start = ads.date_start
AND CAST(shopeeSearch.account_id AS STRING) = CAST(ads.idtkqc AS STRING)