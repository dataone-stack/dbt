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

-- Pre-aggregate doanhThuLadi to avoid duplication by tkqc
ladi_aggregated AS (
  SELECT
    ladi.date_insert AS date_start,
    ladi.staff_id AS ma_nhan_vien,
    ladi.manager,
    ladi.brand,
    ladi.channel,
    SUM(ladi.doanhThuLadi) AS doanhThuLadi
  FROM {{ ref('t2_ladipage_facebook_total') }} AS ladi
  GROUP BY
    ladi.date_insert,
    ladi.staff_id,
    ladi.manager,
    ladi.brand,
    ladi.channel
),

ads_ladipageFacebook_total_with_tkqc AS (
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
    ladi.doanhThuLadi,
    ads.revenue_type
  FROM ads_total_with_tkqc AS ads
  LEFT JOIN ladi_aggregated AS ladi
    ON ads.date_start = ladi.date_start
    AND ads.ma_nhan_vien = ladi.ma_nhan_vien
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