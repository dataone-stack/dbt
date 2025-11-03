WITH ads_total AS (
  SELECT 
    TRIM(brand) AS brand,
    "All" AS channel,
    date_start,
    TRIM(ma_nhan_vien) AS ma_nhan_vien,
    TRIM(staff) AS staff,
    TRIM(ma_quan_ly) AS ma_quan_ly,
    TRIM(manager) AS manager,
    TRIM(company) AS company,
    SUM(doanhThuads + doanh_so_moi) AS DoanhThuAds,
    SUM(chiPhiAds) AS chiPhiAds,
    ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanh_so_moi)), 4) AS cir
  FROM {{ref('t3_ads_total_with_tkqc')}}
  WHERE company = 'Max Eagle' AND DATE(date_start) >= "2025-10-01"
  GROUP BY 1,2,3,4,5,6,7,8
),
kpi_total AS (
  SELECT 
    TRIM(brand) AS brand,
    "All" AS channel,
    TRIM(ma_nhan_vien) AS ma_nhan_vien,
    TRIM(staff) AS staff,
    TRIM(manager_code) AS manager_code,
    TRIM(manager) AS manager,
    TRIM(company) AS company,
    month,
    year,
    revenue_target,
    spend,
    cir_target
  FROM {{ref('t1_kpi_ads_total')}}
  WHERE company = 'Max Eagle' AND month >= 10
),
a AS (
  SELECT 
    -- Merge columns từ cả 2 bảng
    COALESCE(b.brand, a.brand) AS brand,
    COALESCE(b.channel, a.channel) AS channel,
    COALESCE(b.company, a.company) AS company,
    
    -- Date: ưu tiên actual date
    COALESCE(a.date_start, DATE(b.year, b.month, 1)) AS date_start,
    
    -- Staff info
    COALESCE(b.ma_nhan_vien, a.ma_nhan_vien) AS ma_nhan_vien,
    COALESCE(b.staff, a.staff) AS staff,
    COALESCE(b.manager_code, a.ma_quan_ly) AS ma_quan_ly,
    COALESCE(b.manager, a.manager) AS manager,
    
    -- Metrics từ ads_total (không dùng SUM vì đã SUM trong CTE)
    COALESCE(a.DoanhThuAds, 0) AS DoanhThuAds,
    COALESCE(a.chiPhiAds, 0) AS chiPhiAds,
    COALESCE(a.cir, 0) AS cir,
    
    -- KPI targets
    COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)) AS kpi_month,
    COALESCE(b.year, EXTRACT(YEAR FROM a.date_start)) AS kpi_year,
    COALESCE(b.revenue_target, 0) AS revenue_target,
    COALESCE(b.spend, 0) AS spend,
    COALESCE(b.cir_target, 0) AS cir_target
    
  FROM kpi_total b
  FULL OUTER JOIN ads_total a
    ON a.brand = b.brand
    AND a.channel = b.channel
    AND a.ma_nhan_vien = b.ma_nhan_vien
    AND a.ma_quan_ly = b.manager_code
    AND a.company = b.company
    AND EXTRACT(MONTH FROM a.date_start) = b.month
    AND EXTRACT(YEAR FROM a.date_start) = b.year
)
SELECT * FROM a
ORDER BY brand, channel, date_start, ma_nhan_vien 