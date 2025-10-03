-- CREATE VIEW `crypto-arcade-453509-i8.pl_reporting.v_pl_auto` AS
WITH 
-- CTE để tính tổng doanh thu theo SKU để phân bổ ads cost
sku_revenue_for_allocation AS (
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    channel,
    order_id,
    sku_code,
    ten_san_pham,
    promotion_type,
    SUM(doanh_thu_ke_toan) as sku_revenue,
    COUNT(DISTINCT order_id) AS count_sku_revenue
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY year, month, brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
),

-- CTE để tính tổng doanh thu theo brand, company, channel, month, year
total_revenue_by_group AS (
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    SUM(sku_revenue) as total_group_revenue,
    COUNT(DISTINCT order_id) AS count_group_order,
  FROM sku_revenue_for_allocation
  GROUP BY year, month, brand, company, channel
),

-- CTE để lấy tổng chi phí ads theo brand, company, channel, month, year
chi_phi_ads_total AS (
  SELECT 
    EXTRACT(YEAR FROM DATE(date_start)) as year,
    EXTRACT(MONTH FROM DATE(date_start)) as month,
    brand,
    company,
    channel,
    ben_thue,
    idtkqc,
    nametkqc,
    SUM(chiPhiAds) as total_ads_cost
  FROM `crypto-arcade-453509-i8.dtm.t3_ads_total_with_tkqc`
  GROUP BY year, month, brand, company, channel, ben_thue, idtkqc, nametkqc
),

-- CTE để tính chi phí ads phân bổ cho từng SKU
chi_phi_ads_allocated AS (
  SELECT 
    s.year,
    s.month,
    s.brand,
    s.company,
    s.channel,
    s.order_id,
    s.sku_code,
    s.ten_san_pham,
    s.promotion_type,
    a.ben_thue,
    a.idtkqc,
    a.nametkqc,
    -- Công thức phân bổ: (doanh thu SKU / tổng doanh thu nhóm) * tổng chi phí ads
    CASE 
      WHEN t.total_group_revenue > 0 
      THEN (s.sku_revenue / t.total_group_revenue) * a.total_ads_cost
      ELSE 0 
    END as allocated_ads_cost,
    CASE 
      WHEN t.count_group_order > 0 
      THEN (s.count_sku_revenue / t.count_group_order) * a.total_ads_cost
      ELSE 0
    END as allocated_ads_cost_count_order
  FROM sku_revenue_for_allocation s
  LEFT JOIN total_revenue_by_group t
    ON s.year = t.year 
    AND s.month = t.month 
    AND s.brand = t.brand 
    AND s.company = t.company
    AND s.channel = t.channel
  LEFT JOIN chi_phi_ads_total a
    ON s.year = a.year 
    AND s.month = a.month 
    AND s.brand = a.brand 
    AND s.company = a.company
    AND s.channel = a.channel
),

-- CTE để tính tổng chi phí biến đổi từ các bảng khác nhau (đã update)
chi_phi_revenue AS (
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    channel,
    SUM(phu_phi) as san_cost,
    ABS(SUM(phi_van_chuyen_thuc_te)) as van_chuyen_cost,
    SUM(gia_von) as gia_von
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY year, month, brand, company, channel
),

-- Tính tổng chi phí biến đổi (đã update để sử dụng ads cost đã phân bổ)
tong_chi_phi_bien_doi AS (
  SELECT 
    COALESCE(a.year, r.year) as year,
    COALESCE(a.month, r.month) as month,
    COALESCE(a.brand, r.brand) as brand,
    COALESCE(a.company, r.company) as company,
    COALESCE(a.channel, r.channel) as channel,
    COALESCE(SUM(a.total_ads_cost), 0) + COALESCE(r.san_cost, 0) + COALESCE(r.van_chuyen_cost, 0) + COALESCE(r.gia_von, 0) as total_chi_phi_bien_doi
  FROM chi_phi_ads_total a
  FULL OUTER JOIN chi_phi_revenue r
    ON a.year = r.year 
    AND a.month = r.month 
    AND a.brand = r.brand 
    AND a.company = r.company
    AND a.channel = r.channel
  GROUP BY year, month, brand, company, channel, r.san_cost, r.van_chuyen_cost, r.gia_von
),

base_data AS (
  -- Layer 0: Doanh Thu bán hàng
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create_order)) as year,
    EXTRACT(MONTH FROM DATE(date_create_order)) as month,
    brand,
    company,
    channel,
    'Layer 0: Doanh Thu Bán Hàng' as layer_name,
    '1. Doanh số bán hàng' as metric_name,
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(gia_ban_daily_total) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create_order)), EXTRACT(MONTH FROM DATE(date_create_order)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
  UNION ALL

  SELECT 
    EXTRACT(YEAR FROM DATE(date_create_order)) as year,
    EXTRACT(MONTH FROM DATE(date_create_order)) as month,
    brand,
    company,
    channel,
    'Layer 0: Doanh Thu Bán Hàng' as layer_name,
    '2. Doanh thu bán hàng' as metric_name,
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(doanh_thu_ke_toan) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create_order)), EXTRACT(MONTH FROM DATE(date_create_order)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
  UNION ALL

  -- Layer 1: Doanh Thu
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    channel,
    'Layer 1: Doanh Thu' as layer_name,
    '1. Doanh số kế toán' as metric_name,
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(gia_ban_daily_total) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 1: Doanh Thu',
    '2. Doanh thu kế toán',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    -- case
    --     when SUM(doanh_thu_ke_toan) < 60000
    --     then 0
    --     else  SUM(doanh_thu_ke_toan)
    -- end as amount,
     SUM(doanh_thu_ke_toan) as amount,
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
  UNION ALL
  
  -- Layer 2: Chi Phí Biến Đổi
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '1. Giá vốn',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    status as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(gia_von) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type,status

  UNION ALL
  
  -- **PHẦN MỚI: Chi phí Ads đã được phân bổ theo SKU**
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '2. Ads (sum revenue)',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    -- ben_thue as attribute_5,
    -- idtkqc as attribute_6,
    -- nametkqc as attribute_7,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    allocated_ads_cost as amount
  FROM chi_phi_ads_allocated
--   WHERE allocated_ads_cost > 0

  UNION ALL
   
  -- **PHẦN MỚI: Chi phí Ads đã được phân bổ theo SKU**
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '2. Ads (count order)',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    -- ben_thue as attribute_5,
    -- idtkqc as attribute_6,
    -- nametkqc as attribute_7,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    allocated_ads_cost_count_order as amount
  FROM chi_phi_ads_allocated
--   WHERE allocated_ads_cost > 0

  UNION ALL

  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '3. Sàn',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(phu_phi) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '4. Vận chuyển',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    ABS(SUM(phi_van_chuyen_thuc_te)) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '5. Khuyến mãi',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(gia_von) as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  WHERE promotion_type = "Quà Tặng"
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
  
  UNION ALL

  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '6. Hoa hồng',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    0 as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel
  
  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    '7. Tài nguyên',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    0 as amount
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel

  UNION ALL
  
  -- Sử dụng CTE để tính tổng chi phí biến đổi
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    'Layer 2: Chi Phí Biến Đổi',
    'Tổng chi phí biến đổi',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total_chi_phi_bien_doi as amount
  FROM tong_chi_phi_bien_doi
)

-- Final SELECT với aggregation
SELECT 
  year,
  month,
  brand,
  company,
  channel,
  layer_name,
  metric_name,
  attribute_1,
  attribute_2,
  attribute_3,
  attribute_4,
  attribute_5,
  attribute_6,
  attribute_7,
  SUM(amount) as amount
FROM base_data
WHERE amount IS NOT NULL 
  AND month >= 6 
  AND year >= 2025
GROUP BY year, month, brand, company, channel, layer_name, metric_name, attribute_1, attribute_2, attribute_3, attribute_4,attribute_5, attribute_6, attribute_7







-- -- CREATE VIEW `crypto-arcade-453509-i8.pl_reporting.v_pl_auto` AS
-- WITH 
-- -- CTE để tính tổng chi phí biến đổi từ các bảng khác nhau
-- chi_phi_ads AS (
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_start)) as year,
--     EXTRACT(MONTH FROM DATE(date_start)) as month,
--     brand,
--     company,
--     channel,
--     sum(chiPhiAds) as ads_cost
--   FROM `crypto-arcade-453509-i8.dtm.t3_ads_total_with_tkqc`
--   GROUP BY year, month, brand, company, channel
-- ),

-- chi_phi_revenue AS (
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)) as year,
--     EXTRACT(MONTH FROM DATE(date_create)) as month,
--     brand,
--     company,
--     channel,
--     sum(phu_phi) as san_cost,
--     abs(sum(phi_van_chuyen_thuc_te)) as van_chuyen_cost,
--      sum(gia_von) as gia_von
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY year, month, brand, company,channel
-- ),

-- -- Tính tổng chi phí biến đổi
-- tong_chi_phi_bien_doi AS (
--   SELECT 
--     COALESCE(a.year, r.year) as year,
--     COALESCE(a.month, r.month) as month,
--     COALESCE(a.brand, r.brand) as brand,
--     COALESCE(a.company, r.company) as company,
--     COALESCE(a.channel, r.channel) as channel,
--     COALESCE(a.ads_cost, 0) + COALESCE(r.san_cost, 0) + COALESCE(r.van_chuyen_cost, 0)+  COALESCE(r.gia_von, 0) as total_chi_phi_bien_doi
--   FROM chi_phi_ads a
--   FULL OUTER JOIN chi_phi_revenue r
--     ON a.year = r.year 
--     AND a.month = r.month 
--     AND a.brand = r.brand 
--     AND a.company = r.company
--     AND a.channel = r.channel
-- ),

-- base_data AS (
--   -- Layer 1: Doanh Thu bán hàng
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create_order)) as year,
--     EXTRACT(MONTH FROM DATE(date_create_order)) as month,
--     brand,
--     company,
--     channel,
--     'Layer 0: Doanh Thu Bán Hàng' as layer_name,
--     '1. Doanh số bán hàng' as metric_name,
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     sum(gia_ban_daily_total) as amount
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create_order)),EXTRACT(MONTH FROM DATE(date_create_order)), brand, company, order_id, sku_code,channel,ten_san_pham, promotion_type
  
--   UNION ALL

--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create_order)) as year,
--     EXTRACT(MONTH FROM DATE(date_create_order)) as month,
--     brand,
--     company,
--     channel,
--     'Layer 0: Doanh Thu Bán Hàng' as layer_name,
--     '2. Doanh thu bán hàng' as metric_name,
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     sum(doanh_thu_ke_toan) as amount
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create_order)),EXTRACT(MONTH FROM DATE(date_create_order)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
--   UNION ALL

--   -- Layer 1: Doanh Thu
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)) as year,
--     EXTRACT(MONTH FROM DATE(date_create)) as month,
--     brand,
--     company,
--     channel,
--     'Layer 1: Doanh Thu' as layer_name,
--     '1. Doanh số kế toán' as metric_name,
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     sum(gia_ban_daily_total) as amount
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
--   UNION ALL
  
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 1: Doanh Thu',
--     '2. Doanh thu kế toán',
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     sum(doanh_thu_ke_toan)
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham,promotion_type
  
--   UNION ALL
  
--   -- Layer 2: Chi Phí Biến Đổi
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '1. Giá vốn',
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     sum(gia_von)
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type

--   UNION ALL
  
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_start)),
--     EXTRACT(MONTH FROM DATE(date_start)),
--     brand,
--     company,
--     channel as channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '2. Ads',
--     ben_thue as attribute_1,
--     idtkqc as attribute_2,
--     nametkqc as attribute_3,
--     "" as attribute_4,
--     sum(chiPhiAds)
--   FROM `crypto-arcade-453509-i8.dtm.t3_ads_total_with_tkqc`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_start)),EXTRACT(MONTH FROM DATE(date_start)), brand, company, ben_thue,idtkqc,nametkqc,channel

--   UNION ALL

--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '3. Sàn',
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     sum(phu_phi)
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
  
--   UNION ALL
  
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '4. Vận chuyển',
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     abs(sum(phi_van_chuyen_thuc_te))
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
  
--   UNION ALL
  
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '5. Khuyến mãi',
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     sum(gia_von)
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   where promotion_type = "Quà Tặng"
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel, order_id,sku_code,ten_san_pham,promotion_type
  
--   UNION ALL

--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '6. Hoa hồng',
--     "" as attribute_1,
--     "" as attribute_2,
--     "" as attribute_3,
--     "" as attribute_4,
--     0 -- sum(phi_hoa_hong_shop) + sum(phi_hoa_hong_tiep_thi_lien_ket) + sum(phi_hoa_hong_quang_cao_cua_hang)
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel
  
--   UNION ALL
  
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '7. Tài nguyên',
--     "" as attribute_1,
--     "" as attribute_2,
--     "" as attribute_3,
--     "" as attribute_4,
--     0 --sum(phi_thanh_toan) + sum(phi_dich_vu) + sum(phi_xtra)
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)),EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel

--   UNION ALL
  
--   -- Sử dụng CTE để tính tổng chi phí biến đổi
--   SELECT 
--     year,
--     month,
--     brand,
--     company,
--     channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     'Tổng chi phí biến đổi',
--     "" as attribute_1,
--     "" as attribute_2,
--     "" as attribute_3,
--     "" as attribute_4,
--     total_chi_phi_bien_doi
--   FROM tong_chi_phi_bien_doi
-- )

-- -- Final SELECT với aggregation
-- SELECT 
--   year,
--   month,
--   brand,
--   company,
--   channel,
--   layer_name,
--   metric_name,
--   attribute_1,
--   attribute_2,
--   attribute_3,
--   attribute_4,
--   SUM(amount) as amount
-- FROM base_data
-- WHERE amount IS NOT NULL and month >= 7 and year >= 2025
-- GROUP BY year, month, brand, company, channel, layer_name, metric_name, attribute_1, attribute_2,attribute_3, attribute_4


