WITH ads_total AS (
  SELECT 
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    company,
    SUM(doanhThuads + doanhThuLadi) AS DoanhThuAds,
    SUM(chiPhiAds) AS chiPhiAds,
    ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanhThuLadi)), 4) AS cir,

  FROM {{ref('t3_ads_total_with_tkqc')}}
  WHERE company = 'Max Eagle'
  GROUP BY
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    company
)

SELECT 
  a.*,
  b.month as kpi_month,
  b.year as kpi_year,
  b.revenue_target,
  b.cir_target
FROM ads_total a
LEFT JOIN {{ref('t1_kpi_ads_total')}} b
  ON a.brand = b.brand
  AND a.channel = b.channel
  AND a.ma_nhan_vien = b.ma_nhan_vien
  AND a.ma_quan_ly = b.manager_code
  AND a.company = b.company
  AND EXTRACT(MONTH FROM a.date_start) = b.month
  AND EXTRACT(YEAR FROM a.date_start) = b.year