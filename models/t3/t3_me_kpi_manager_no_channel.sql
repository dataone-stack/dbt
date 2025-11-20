WITH 
ads_total AS (
  SELECT 
    TRIM(brand) AS brand,
    date_start,
    TRIM(manager) AS manager,
    SUM(doanh_thu_ke_toan) AS doanh_thu_ke_toan,
    SUM(doanhThuAds + doanh_so_moi + doanh_so_cu) AS doanh_so_tong,
    SUM(doanhThuAds + doanh_so_moi) AS doanh_so_moi,
    SUM(doanh_so_cu) AS doanh_so_cu,
    SUM(chi_phi_ads) AS chi_phi_ads,
    ROUND(SAFE_DIVIDE(SUM(chi_phi_ads), SUM(doanhThuAds + doanh_so)), 4) AS cir
  FROM {{ ref('t4_me_revenue_and_cost_toa') }}
  WHERE company = 'Max Eagle' and DATE(date_start) >= '2025-11-01'
  GROUP BY date_start, brand, manager
),

kpi_total AS (
  SELECT 
    TRIM(brand) AS brand,
    TRIM(manager_code) AS ma_quan_ly,
    TRIM(manager) AS manager,
    month,
    year,
    SUM(kpi_doanh_so_tong) AS kpi_doanh_so_tong,
    SUM(kpi_doanh_so_moi) AS kpi_doanh_so_moi,
    SUM(kpi_doanh_so_cu) AS kpi_doanh_so_cu,
    SUM(kpi_chi_tieu) AS kpi_chi_tieu,
    ROUND(SAFE_DIVIDE(SUM(kpi_chi_tieu), SUM(kpi_doanh_so_moi)), 4) AS kpi_cir_moi,
    ROUND(SAFE_DIVIDE(SUM(kpi_chi_tieu), SUM(kpi_doanh_so_tong)), 4) AS kpi_cir
  FROM `google_sheet.me_kpi_manager`
  WHERE year IS NOT NULL
  GROUP BY month, year, brand, manager, ma_quan_ly
),

-- BƯỚC QUAN TRỌNG: Tạo bộ khung Ngày cho từng Brand/Manager dựa trên KPI tháng
kpi_daily_expanded AS (
  SELECT
    k.*,
    day_date AS date_start_spine
  FROM kpi_total k,
  -- Tạo mảng ngày từ ngày đầu tháng đến ngày cuối tháng (hoặc ngày hiện tại)
  UNNEST(GENERATE_DATE_ARRAY(DATE(k.year, k.month, 1), LAST_DAY(DATE(k.year, k.month, 1)))) AS day_date
  -- Lọc để chỉ lấy các ngày nằm trong khoảng thời gian báo cáo (nếu cần)
  -- WHERE day_date >= '2025-11-01'
),

final_table AS (
  SELECT 
    -- Merge columns từ cả 2 bảng
    COALESCE(b.brand, a.brand) AS brand,
    COALESCE(a.date_start, b.date_start_spine) AS date_start,
    COALESCE(b.manager, a.manager) AS manager,
    
    -- Metrics từ ads_total (không dùng SUM vì đã SUM trong CTE)
    COALESCE(a.doanh_so_tong, 0) AS doanh_so_tong,
    COALESCE(a.doanh_so_moi, 0) AS doanh_so_moi,
    COALESCE(a.doanh_thu_ke_toan, 0) AS doanh_thu_ke_toan,
    COALESCE(a.doanh_so_cu, 0) AS doanh_so_cu,
    COALESCE(a.chi_phi_ads, 0) AS chi_phi_ads,
    COALESCE(a.cir, 0) AS cir,
    
    -- KPI targets
    COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)) AS kpi_month,
    COALESCE(b.year, EXTRACT(YEAR FROM a.date_start)) AS kpi_year,
    COALESCE(b.kpi_doanh_so_tong, 0) AS kpi_doanh_so_tong,
    COALESCE(b.kpi_doanh_so_moi, 0) AS kpi_doanh_so_moi,
    COALESCE(b.kpi_doanh_so_cu, 0) AS kpi_doanh_so_cu,
    COALESCE(b.kpi_chi_tieu, 0) AS kpi_chi_tieu,
    COALESCE(b.kpi_cir_moi, 0) AS kpi_cir_moi,
    COALESCE(b.kpi_cir, 0) AS kpi_cir
    
  FROM kpi_daily_expanded b
  FULL OUTER JOIN ads_total a
    ON a.brand = b.brand
    AND a.manager = b.manager
    AND b.date_start_spine = a.date_start
)
SELECT * FROM final_table
ORDER BY brand, date_start