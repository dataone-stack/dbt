WITH ads_total AS (
    SELECT 
    TRIM(brand) AS brand,
    TRIM(channel) AS channel,
    date_start,
    TRIM(ma_nhan_vien) AS ma_nhan_vien,
    TRIM(staff) AS staff,  -- FIX: TRIM để loại bỏ spaces
    TRIM(ma_quan_ly) AS ma_quan_ly,
    TRIM(manager) AS manager,
    TRIM(company) AS company,
    COALESCE(SUM(doanhThuads + doanh_so_moi), 0) AS DoanhThuAds,
    COALESCE(SUM(chiPhiAds),0) AS chiPhiAds,
    COALESCE(ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanh_so_moi)), 4),0) AS cir,

  FROM {{ref('t3_ads_total_with_tkqc')}}
  WHERE company = 'Max Eagle' 
  GROUP BY
    1,2,3,4,5,6,7,8
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
  SUM(a.DoanhThuAds) AS DoanhThuAds,
  SUM(a.chiPhiAds) AS chiPhiAds,
  MAX(a.cir) AS cir,
  
  -- Data từ KPI
  COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)) AS kpi_month,
  COALESCE(b.year, EXTRACT(YEAR FROM a.date_start)) AS kpi_year,
  COALESCE(SUM(b.revenue_target),0) AS revenue_target,
  COALESCE(SUM(b.spend),0) AS spend,
  COALESCE(MAX(b.cir_target),0) AS cir_target
  
FROM {{ref('t1_kpi_ads_total')}} b
LEFT JOIN ads_total a
  ON a.brand = b.brand
  AND a.channel = b.channel
  AND a.ma_nhan_vien = b.ma_nhan_vien
  AND a.ma_quan_ly = b.manager_code
  AND a.company = b.company
  AND EXTRACT(MONTH FROM a.date_start) = b.month
  AND EXTRACT(YEAR FROM a.date_start) = b.year
WHERE b.company = 'Max Eagle'
GROUP BY
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    company,
    COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)),
    COALESCE(b.year, EXTRACT(YEAR FROM a.date_start))
)
select * from a --where ma_nhan_vien = "NTB000157" and brand = "An Cung" -- and date_start between "2025-10-01" and "2025-10-31"






-- WITH ads_total AS (
--   SELECT 
--     brand_lv1 AS brand,
--     channel,
--     date_start,
--     ma_nhan_vien,
--     staff,
--     ma_quan_ly,
--     manager,
--     company,
--     COALESCE(SUM(doanhThuads + doanh_so_moi), 0) AS DoanhThuAds,
--     COALESCE(SUM(chiPhiAds),0) AS chiPhiAds,
--     COALESCE(ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanh_so_moi)), 4),0) AS cir,

--   FROM {{ref('t3_ads_total_with_tkqc')}}
--   WHERE company = 'Max Eagle' 
--   GROUP BY
--     brand_lv1,
--     channel,
--     date_start,
--     ma_nhan_vien,
--     staff,
--     ma_quan_ly,
--     manager,
--     company
-- )
-- SELECT 
--   a.brand,
--   a.channel,
--   a.date_start,
--   a.ma_nhan_vien,
--   a.staff,
--   a.ma_quan_ly,
--   a.manager,
--   a.company,
--   SUM(a.DoanhThuAds) AS DoanhThuAds,
--   SUM(a.chiPhiAds) AS chiPhiAds,
--   MAX(a.cir) AS cir,

--   COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)) AS kpi_month,
--   COALESCE(b.year, EXTRACT(YEAR FROM a.date_start)) AS kpi_year,
--   COALESCE(SUM(b.revenue_target),0) AS revenue_target,
--   COALESCE(SUM(b.spend),0) AS spend,
--   COALESCE(MAX(b.cir_target),0) AS cir_target
-- FROM ads_total a
-- FULL OUTER JOIN {{ref('t1_kpi_ads_total')}} b
--   ON TRIM(a.brand) = TRIM(b.brand)
--   AND TRIM(a.channel) = TRIM(b.channel)
--   AND TRIM(a.ma_nhan_vien) = TRIM(b.ma_nhan_vien)
--   AND TRIM(a.ma_quan_ly) = TRIM(b.manager_code)
--   AND TRIM(a.company) = TRIM(b.company)
--   AND EXTRACT(MONTH FROM a.date_start) = b.month
--   AND EXTRACT(YEAR FROM a.date_start) = b.year
--   GROUP BY
--     brand,
--     channel,
--     date_start,
--     ma_nhan_vien,
--     staff,
--     ma_quan_ly,
--     manager,
--     company,
--     COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)),
--     COALESCE(b.year, EXTRACT(YEAR FROM a.date_start))