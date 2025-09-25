
-- CREATE VIEW `crypto-arcade-453509-i8.pl_reporting.v_pl_auto` AS
WITH 
-- CTE để tính tổng chi phí biến đổi từ các bảng khác nhau
chi_phi_ads AS (
  SELECT 
    EXTRACT(YEAR FROM DATE(date_start)) as year,
    EXTRACT(MONTH FROM DATE(date_start)) as month,
    brand,
    company,
    sum(chiPhiAds) as ads_cost
  FROM `crypto-arcade-453509-i8.dtm.t3_ads_total_with_tkqc`
  GROUP BY year, month, brand, company
),

chi_phi_revenue AS (
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    sum(phu_phi) as san_cost,
    abs(sum(phi_van_chuyen_thuc_te)) as van_chuyen_cost,
     sum(gia_von) as gia_von
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY year, month, brand, company
),

-- Tính tổng chi phí biến đổi
tong_chi_phi_bien_doi AS (
  SELECT 
    COALESCE(a.year, r.year) as year,
    COALESCE(a.month, r.month) as month,
    COALESCE(a.brand, r.brand) as brand,
    COALESCE(a.company, r.company) as company,
    COALESCE(a.ads_cost, 0) + COALESCE(r.san_cost, 0) + COALESCE(r.van_chuyen_cost, 0)+  COALESCE(r.gia_von, 0) as total_chi_phi_bien_doi
  FROM chi_phi_ads a
  FULL OUTER JOIN chi_phi_revenue r
    ON a.year = r.year 
    AND a.month = r.month 
    AND a.brand = r.brand 
    AND a.company = r.company
),

base_data AS (
  -- Layer 1: Doanh Thu bán hàng
  SELECT 
    EXTRACT(YEAR FROM DATE(ngay_tao_don)) as year,
    EXTRACT(MONTH FROM DATE(ngay_tao_don)) as month,
    brand,
    company,
    'Layer 0: Doanh Thu Bán Hàng' as layer_name,
    '1. Doanh số bán hàng' as metric_name,
    sum(gia_ban_daily_total) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_revenue_all_channel`
  GROUP BY EXTRACT(YEAR FROM DATE(ngay_tao_don)),EXTRACT(MONTH FROM DATE(ngay_tao_don)), brand, company
  
  UNION ALL

  SELECT 
    EXTRACT(YEAR FROM DATE(ngay_tao_don)) as year,
    EXTRACT(MONTH FROM DATE(ngay_tao_don)) as month,
    brand,
    company,
    'Layer 0: Doanh Thu Bán Hàng' as layer_name,
    '2. Doanh thu bán hàng' as metric_name,
    sum(doanh_thu_ke_toan) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_revenue_all_channel`
  GROUP BY EXTRACT(YEAR FROM DATE(ngay_tao_don)),EXTRACT(MONTH FROM DATE(ngay_tao_don)), brand, company
  
  UNION ALL

  -- Layer 1: Doanh Thu
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    'Layer 1: Doanh Thu' as layer_name,
    '1. Doanh số kế toán' as metric_name,
    sum(gia_ban_daily_total) as amount
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    'Layer 1: Doanh Thu',
    '2. Doanh thu kế toán',
    sum(doanh_thu_ke_toan)
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company
  
  UNION ALL
  
  -- Layer 2: Chi Phí Biến Đổi
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    'Layer 2: Chi Phí Biến Đổi',
    '1. Giá vốn',
    sum(gia_von)
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company

  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_start)),
    EXTRACT(MONTH FROM DATE(date_start)),
    brand,
    company,
    'Layer 2: Chi Phí Biến Đổi',
    '2. Ads',
    sum(chiPhiAds)
  FROM `crypto-arcade-453509-i8.dtm.t3_ads_total_with_tkqc`
  GROUP BY EXTRACT(YEAR FROM DATE(date_start)),EXTRACT(MONTH FROM DATE(date_start)), brand, company

  UNION ALL

  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    'Layer 2: Chi Phí Biến Đổi',
    '3. Sàn',
    sum(phu_phi)
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    'Layer 2: Chi Phí Biến Đổi',
    '4. Vận chuyển',
    abs(sum(phi_van_chuyen_thuc_te))
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    'Layer 2: Chi Phí Biến Đổi',
    '5. Hoa hồng',
    0 -- sum(phi_hoa_hong_shop) + sum(phi_hoa_hong_tiep_thi_lien_ket) + sum(phi_hoa_hong_quang_cao_cua_hang)
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    'Layer 2: Chi Phí Biến Đổi',
    '6. Tài nguyên',
    0 --sum(phi_thanh_toan) + sum(phi_dich_vu) + sum(phi_xtra)
  FROM `crypto-arcade-453509-i8.dtm.pnl_t3_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company

  UNION ALL
  
  -- Sử dụng CTE để tính tổng chi phí biến đổi
  SELECT 
    year,
    month,
    brand,
    company,
    'Layer 2: Chi Phí Biến Đổi',
    'Tổng chi phí biến đổi',
    total_chi_phi_bien_doi
  FROM tong_chi_phi_bien_doi
)

-- Final SELECT với aggregation
SELECT 
  year,
  month,
  brand,
  company,
  layer_name,
  metric_name,
  SUM(amount) as amount
FROM base_data
WHERE amount IS NOT NULL
GROUP BY year, month, brand, company, layer_name, metric_name


