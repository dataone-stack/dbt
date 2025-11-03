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
    ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanhThuLadi)), 4) AS cir
  FROM  {{ref('t3_ads_total_with_tkqc')}}
  WHERE company = 'One5'
  GROUP BY 1,2,3,4,5,6,7,8
),
a as(
SELECT 
  -- Ưu tiên lấy từ KPI table
  COALESCE(b.brand, a.brand) AS brand,
  COALESCE(b.channel, a.channel) AS channel,
  COALESCE(b.company, a.company) AS company,
  
  -- Data từ ads_total
  COALESCE(a.date_start, DATE(b.year, b.month, 1)) AS date_start,
  -- a.date_start,
  COALESCE(b.ma_nhan_vien, a.ma_nhan_vien) AS ma_nhan_vien,
  COALESCE(b.staff, a.staff) AS staff,
  COALESCE(b.manager_code, a.ma_quan_ly) AS ma_quan_ly,
  COALESCE(b.manager, a.manager) AS manager,
--   a.date_start,
--   a.ma_nhan_vien,
--   a.staff,
--   a.ma_quan_ly,
--   a.manager,
  COALESCE(a.DoanhThuAds, 0) AS DoanhThuAds,
  COALESCE(a.chiPhiAds, 0) AS chiPhiAds,
  COALESCE(a.cir, 0) AS cir,
  
  -- Data từ KPI
  b.month AS kpi_month,
  b.year AS kpi_year,
  COALESCE(b.revenue_target, 0) AS revenue_target,
  COALESCE(b.spend, 0) AS spend,
  COALESCE(b.cir_target, 0) AS cir_target
  
FROM {{ref('t1_kpi_ads_total')}} b
LEFT JOIN ads_total a
  ON a.brand = b.brand
  AND a.channel = b.channel
  AND a.ma_nhan_vien = b.ma_nhan_vien
  AND a.ma_quan_ly = b.manager_code
  AND a.company = b.company
  AND EXTRACT(MONTH FROM a.date_start) = b.month
  AND EXTRACT(YEAR FROM a.date_start) = b.year
WHERE b.company = 'One5'
)
select * from a -- where ma_nhan_vien = "NTB000157" and brand = "An Cung" -- and date_start between "2025-10-01" and "2025-10-31"



-- WITH ads_total AS (
--   SELECT 
--     brand,
--     channel,
--     date_start,
--     ma_nhan_vien,
--     staff,
--     ma_quan_ly,
--     manager,
--     company,
--     SUM(doanhThuads + doanhThuLadi) AS DoanhThuAds,
--     SUM(chiPhiAds) AS chiPhiAds,
--     ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanhThuLadi)), 4) AS cir,

--   FROM {{ref('t3_ads_total_with_tkqc')}}
--   WHERE company = 'One5'
--   GROUP BY
--     brand,
--     channel,
--     date_start,
--     ma_nhan_vien,
--     staff,
--     ma_quan_ly,
--     manager,
--     company
-- )

-- SELECT 
--   a.*,
--   COALESCE(b.month,0) AS kpi_month,
--   COALESCE(b.year,0) AS kpi_year,
--   COALESCE(b.revenue_target,0) AS revenue_target,
--   COALESCE(b.spend,0) AS spend,
--   COALESCE(b.cir_target,0) AS cir_target
-- FROM ads_total a
-- FULL OUTER JOIN {{ref('t1_kpi_ads_total')}} b
--   ON a.brand = b.brand
--   AND a.channel = b.channel
--   AND a.ma_nhan_vien = b.ma_nhan_vien
--   AND a.ma_quan_ly = b.manager_code
--   AND a.company = b.company
--   AND EXTRACT(MONTH FROM a.date_start) = b.month
--   AND EXTRACT(YEAR FROM a.date_start) = b.year