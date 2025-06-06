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

-- Aggregate ads data at staff level for joining with ladi
ads_staff_level AS (
  SELECT
    date_start,
    ma_nhan_vien,
    staff,
    manager,
    brand,
    channel,
    revenue_type,
    SUM(chiPhiAds) AS chiPhiAds,
    SUM(doanhThuAds) AS doanhThuAds
  FROM ads_total_with_tkqc
  GROUP BY
    date_start,
    ma_nhan_vien,
    staff,
    manager,
    brand,
    channel,
    revenue_type
),

-- Join with ladi at staff level
ads_ladipageFacebook_total_with_tkqc AS (
  SELECT
    ads.date_start,
    tkqc.idtkqc,
    tkqc.nametkqc,
    tkqc.ma_nhan_vien,
    tkqc.staff,
    tkqc.manager,
    tkqc.brand,
    tkqc.channel,
    tkqc.chiPhiAds,
    tkqc.doanhThuAds,
    ladi.doanhThuLadi,
    tkqc.revenue_type
  FROM ads_total_with_tkqc AS tkqc
  LEFT JOIN ads_staff_level AS ads
    ON tkqc.date_start = ads.date_start
    AND tkqc.ma_nhan_vien = ads.ma_nhan_vien
    AND tkqc.manager = ads.manager
    AND tkqc.brand = ads.brand
    AND tkqc.channel = ads.channel
    AND tkqc.revenue_type = ads.revenue_type
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