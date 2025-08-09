WITH ads_total AS (
  SELECT 
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    SUM(doanhThuads + doanhThuLadi) AS DoanhThuAds,
    SUM(chiPhiAds) AS chiPhiAds,
    ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanhThuLadi)), 4) AS cir,

    -- ✅ Đếm số dòng để phân bổ KPI
    COUNT(*) OVER (
      PARTITION BY brand, channel, ma_nhan_vien, ma_quan_ly
    ) AS num_rows

  FROM {{ref('t3_ads_total_with_tkqc')}}
  WHERE company = 'One5' AND DATE(date_start) > '2025-07-31'
  GROUP BY
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager
)

SELECT 
  a.*,
  b.month as kpi_month,
  b.year as kpi_year,
  b.revenue_target,
  SAFE_DIVIDE(b.revenue_target, a.num_rows) AS revenue_target_for_row_level_sum,
  b.cir_target
FROM ads_total a
LEFT JOIN {{ref('t1_kpi_ads_total')}} b
  ON a.brand = b.brand
  AND a.channel = b.channel
  AND a.ma_nhan_vien = b.ma_nhan_vien
  AND a.ma_quan_ly = b.manager_code
  AND EXTRACT(MONTH FROM a.date_start) = b.month
  AND EXTRACT(YEAR FROM a.date_start) = b.year