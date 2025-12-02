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
    SUM(doanh_thu_ke_toan) as sku_revenue
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY year, month, brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
),
-- Thêm CTE mới để tính doanh thu theo đơn hàng:
order_revenue AS (
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    order_id,
    SUM(sku_revenue) as order_revenue
  FROM sku_revenue_for_allocation
  GROUP BY year, month, brand, company, channel, order_id
),
-- CTE để tính doanh thu kế toán theo group chi tiết - SỬA LẠI
doanh_thu_ke_toan_base AS (
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    channel,
    order_id,
    sku_code,
    promotion_type,
    SUM(doanh_thu_ke_toan) as total_revenue_base
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY year, month, brand, company, channel, order_id, sku_code, promotion_type
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
    COUNT(DISTINCT order_id) AS total_order_count
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
    SUM(chiPhiAds) as total_ads_cost
  FROM `crypto-arcade-453509-i8.dtm.t3_ads_total_with_tkqc`
  GROUP BY year, month, brand, company, channel
),


sku_count_per_order AS (
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    order_id,
    COUNT(DISTINCT sku_code) as sku_count_in_order
  FROM sku_revenue_for_allocation
  GROUP BY year, month, brand, company, channel, order_id
),

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
    -- Phương thức 1: Theo doanh thu SKU
    CASE 
      WHEN t.total_group_revenue > 0 
      THEN (s.sku_revenue / t.total_group_revenue) * a.total_ads_cost
      ELSE 0 
    END as allocated_ads_cost,
    -- Phương thức 2: Chia đều cho đơn hàng, sau đó chia cho số SKU
    CASE 
      WHEN t.total_order_count > 0 AND sc.sku_count_in_order > 0
      THEN (a.total_ads_cost / t.total_order_count) / sc.sku_count_in_order
      ELSE 0
    END as allocated_ads_cost_count_order
  FROM sku_revenue_for_allocation s
  LEFT JOIN total_revenue_by_group t
    ON s.year = t.year 
    AND s.month = t.month 
    AND s.brand = t.brand 
    AND s.company = t.company
    AND s.channel = t.channel
  LEFT JOIN order_revenue o
    ON s.year = o.year 
    AND s.month = o.month 
    AND s.brand = o.brand 
    AND s.company = o.company
    AND s.channel = o.channel
    AND s.order_id = o.order_id
  LEFT JOIN chi_phi_ads_total a
    ON s.year = a.year 
    AND s.month = a.month 
    AND s.brand = a.brand 
    AND s.company = a.company
    AND s.channel = a.channel
  LEFT JOIN sku_count_per_order sc
    ON s.year = sc.year 
    AND s.month = sc.month 
    AND s.brand = sc.brand 
    AND s.company = sc.company
    AND s.channel = sc.channel
    AND s.order_id = sc.order_id
),


-- CTE để tính tổng chi phí biến đổi từ các bảng khác nhau
chi_phi_revenue AS (
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    channel,
    SUM(phu_phi) as san_cost,
    ABS(SUM(phi_van_chuyen_thuc_te)) as van_chuyen_cost,
    SUM(gia_von_total) as gia_von
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY year, month, brand, company, channel
),

--tính chi phi co dinh (%)
pl_agg AS (
  SELECT
    EXTRACT(YEAR  FROM DATE(date_create)) AS year,
    EXTRACT(MONTH FROM DATE(date_create)) AS month,
    brand,
    company,
    SUM(gia_ban_daily_total_final) AS gia_ban_final_sum,
    SUM(gia_ban_daily_total)       AS gia_ban_sum
  FROM `dtm.t3_pnl_revenue`
  GROUP BY year, month, brand, company
),

phi_co_dinh_percent AS (
  SELECT
    pcd.year,
    pcd.month,
    pcd.brand,
    pcd.company,
    pcd.layer1,
    pcd.layer2,

    CASE 
      WHEN pcd.type_of_calculation = 'percent' 
      THEN pl_agg.gia_ban_final_sum * pcd.total
      ELSE pcd.total 
    END as total
  FROM `dtm.t1_phi_co_dinh` pcd
  LEFT JOIN pl_agg
    ON pcd.year    = pl_agg.year
    AND pcd.month   = pl_agg.month
    AND pcd.brand   = pl_agg.brand
    AND pcd.company = pl_agg.company
  -- WHERE pcd.type_of_calculation = 'percent'
),

-- Tính tổng chi phí biến đổi
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

phi_co_dinh_sum AS (
  SELECT
    year,
    month,
    brand,
    company,
    -- Nếu có channel thì thêm channel
    SUM(total) AS total_phi_co_dinh
  FROM phi_co_dinh_percent
  GROUP BY year, month, brand, company --, channel nếu có
),

-- CTE tính tỷ lệ phần trăm
ty_le_phan_tram AS (
  SELECT 
    r.year,
    r.month,
    r.brand,
    r.company,
    r.channel,
    r.total_group_revenue,
    c.total_chi_phi_bien_doi,
    r.total_group_revenue - c.total_chi_phi_bien_doi as net_income,
    -- Tính Net Profit Margin %
    CASE 
      WHEN r.total_group_revenue > 0 
      THEN ROUND(((r.total_group_revenue - c.total_chi_phi_bien_doi) / r.total_group_revenue) * 100, 2)
      ELSE 0 
    END as net_profit_margin_pct,
    -- Tính Cost Ratio %
    CASE 
      WHEN r.total_group_revenue > 0 
      THEN ROUND((c.total_chi_phi_bien_doi / r.total_group_revenue) * 100, 2)
      ELSE 0 
    END as cost_ratio_pct
  FROM total_revenue_by_group r
  LEFT JOIN tong_chi_phi_bien_doi c
    ON r.year = c.year 
    AND r.month = c.month 
    AND r.brand = c.brand 
    AND r.company = c.company
    AND r.channel = c.channel
),
ty_le_phan_tram_co_dinh AS (
  SELECT 
    r.year,
    r.month,
    r.brand,
    r.company,
    r.channel,
    r.total_group_revenue,
    c.total_chi_phi_bien_doi,
    COALESCE(p.total_phi_co_dinh, 0) AS total_phi_co_dinh,
    r.total_group_revenue - c.total_chi_phi_bien_doi - COALESCE(p.total_phi_co_dinh, 0) AS net_income,
    CASE 
      WHEN r.total_group_revenue > 0 
      THEN ROUND(((r.total_group_revenue - c.total_chi_phi_bien_doi - COALESCE(p.total_phi_co_dinh, 0)) / r.total_group_revenue) * 100, 2)
      ELSE 0
    END AS net_profit_margin_pct,
    CASE
      WHEN r.total_group_revenue > 0
      THEN ROUND(((c.total_chi_phi_bien_doi + COALESCE(p.total_phi_co_dinh, 0)) / r.total_group_revenue) * 100, 2)
      ELSE 0
    END AS cost_ratio_pct
  FROM total_revenue_by_group r
  LEFT JOIN tong_chi_phi_bien_doi c
    ON r.year = c.year 
    AND r.month = c.month 
    AND r.brand = c.brand 
    AND r.company = c.company
    AND r.channel = c.channel
  LEFT JOIN phi_co_dinh_sum p
    ON r.year = p.year
    AND r.month = p.month
    AND r.brand = p.brand
    AND r.company = p.company
    -- Nếu có channel thì thêm điều kiện channel
    -- AND r.channel = p.channel
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
    SUM(gia_ban_daily_total) as amount,
    0  as percent
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
    SUM(doanh_thu_ke_toan) as amount,
    0  as percent
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
    SUM(gia_ban_daily_total) as amount,
    0  as percent
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
  UNION ALL

  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)) as year,
    EXTRACT(MONTH FROM DATE(date_create)) as month,
    brand,
    company,
    channel,
    'Layer 1: Doanh Thu' as layer_name,
    '2. Chiết khấu' as metric_name,
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(tien_chiet_khau_sp_tot) as amount,
    0  as percent
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
    '3. Doanh thu kế toán',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    status as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(doanh_thu_ke_toan) as amount,
    0  as percent
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type, status


  UNION ALL
  
  SELECT 
    EXTRACT(YEAR FROM DATE(date_create)),
    EXTRACT(MONTH FROM DATE(date_create)),
    brand,
    company,
    channel,
    'Layer 1: Doanh Thu',
    '3.1 Hoàn',
    order_id as attribute_1,
    sku_code as attribute_2,
    ten_san_pham as attribute_3,
    promotion_type as attribute_4,
    status as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    CASE
        WHEN status = "Đã hoàn"
        THEN SUM(doanh_thu_ke_toan)
        ELSE 0
    END as amount,
    0  as percent
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type, status
  
  UNION ALL
  
  -- Layer 2: Chi Phí Biến Đổi với percentage - ĐÃ SỬA
  -- 1. Giá vốn
  SELECT 
    EXTRACT(YEAR FROM DATE(r.date_create)) as year,
    EXTRACT(MONTH FROM DATE(r.date_create)) as month,
    r.brand,
    r.company,
    r.channel,
    'Layer 2: Chi Phí Biến Đổi',
    '1. Giá vốn',
    r.order_id as attribute_1,
    r.sku_code as attribute_2,
    r.ten_san_pham as attribute_3,
    r.promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(r.gia_von_total) as amount,
    -- Tính % giá vốn trên doanh thu kế toán
    CASE 
        WHEN b.total_revenue_base > 0 
        THEN ROUND((SUM(r.gia_von_total) / b.total_revenue_base) * 100, 2)
        ELSE 0 
    END as percent
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue` r
  LEFT JOIN doanh_thu_ke_toan_base b
    ON EXTRACT(YEAR FROM DATE(r.date_create)) = b.year 
    AND EXTRACT(MONTH FROM DATE(r.date_create)) = b.month 
    AND r.brand = b.brand 
    AND r.company = b.company
    AND r.channel = b.channel
    AND r.order_id = b.order_id 
    AND r.sku_code = b.sku_code
    AND r.promotion_type = b.promotion_type
  GROUP BY EXTRACT(YEAR FROM DATE(r.date_create)), EXTRACT(MONTH FROM DATE(r.date_create)), r.brand, r.company, r.order_id, r.sku_code, r.channel, r.ten_san_pham, r.promotion_type, b.total_revenue_base


  UNION ALL


  -- 2. Ads (sum revenue) - ĐÃ SỬA
  SELECT 
    a.year,
    a.month,
    a.brand,
    a.company,
    a.channel,
    'Layer 2: Chi Phí Biến Đổi',
    '2. Ads (sum revenue)',
    a.order_id as attribute_1,
    a.sku_code as attribute_2,
    a.ten_san_pham as attribute_3,
    a.promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    a.allocated_ads_cost as amount,
    -- Tính % ads cost trên doanh thu kế toán
    CASE 
        WHEN b.total_revenue_base > 0 
        THEN ROUND((a.allocated_ads_cost / b.total_revenue_base) * 100, 2)
        ELSE 0 
    END as percent
  FROM chi_phi_ads_allocated a
  LEFT JOIN doanh_thu_ke_toan_base b
    ON a.year = b.year 
    AND a.month = b.month 
    AND a.brand = b.brand 
    AND a.company = b.company
    AND a.channel = b.channel
    AND a.order_id = b.order_id 
    AND a.sku_code = b.sku_code
    AND a.promotion_type = b.promotion_type


  UNION ALL


  -- 2b. Ads (count order) - ĐÃ SỬA
  SELECT 
    a.year,
    a.month,
    a.brand,
    a.company,
    a.channel,
    'Layer 2: Chi Phí Biến Đổi',
    '2b. Ads (count order)',
    a.order_id as attribute_1,
    a.sku_code as attribute_2,
    a.ten_san_pham as attribute_3,
    a.promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    a.allocated_ads_cost_count_order as amount,
    -- Tính % ads cost (theo đơn) trên doanh thu kế toán
    CASE 
        WHEN b.total_revenue_base > 0 
        THEN ROUND((a.allocated_ads_cost_count_order / b.total_revenue_base) * 100, 2)
        ELSE 0 
    END as percent
  FROM chi_phi_ads_allocated a
  LEFT JOIN doanh_thu_ke_toan_base b
    ON a.year = b.year 
    AND a.month = b.month 
    AND a.brand = b.brand 
    AND a.company = b.company
    AND a.channel = b.channel
    AND a.order_id = b.order_id 
    AND a.sku_code = b.sku_code
    AND a.promotion_type = b.promotion_type


  UNION ALL


  -- 3. Sàn - ĐÃ SỬA
  SELECT 
    EXTRACT(YEAR FROM DATE(r.date_create)) as year,
    EXTRACT(MONTH FROM DATE(r.date_create)) as month,
    r.brand,
    r.company,
    r.channel,
    'Layer 2: Chi Phí Biến Đổi',
    '3. Sàn',
    r.order_id as attribute_1,
    r.sku_code as attribute_2,
    r.ten_san_pham as attribute_3,
    r.promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    SUM(r.phu_phi) as amount,
    -- Tính % phí sàn trên doanh thu kế toán
    CASE 
        WHEN b.total_revenue_base > 0 
        THEN ROUND((SUM(r.phu_phi) / b.total_revenue_base) * 100, 2)
        ELSE 0 
    END as percent
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue` r
  LEFT JOIN doanh_thu_ke_toan_base b
    ON EXTRACT(YEAR FROM DATE(r.date_create)) = b.year 
    AND EXTRACT(MONTH FROM DATE(r.date_create)) = b.month 
    AND r.brand = b.brand 
    AND r.company = b.company
    AND r.channel = b.channel
    AND r.order_id = b.order_id 
    AND r.sku_code = b.sku_code
    AND r.promotion_type = b.promotion_type
  GROUP BY EXTRACT(YEAR FROM DATE(r.date_create)), EXTRACT(MONTH FROM DATE(r.date_create)), r.brand, r.company, r.channel, r.order_id, r.sku_code, r.ten_san_pham, r.promotion_type, b.total_revenue_base


  UNION ALL


  -- 4. Vận chuyển - ĐÃ SỬA
  SELECT 
    EXTRACT(YEAR FROM DATE(r.date_create)) as year,
    EXTRACT(MONTH FROM DATE(r.date_create)) as month,
    r.brand,
    r.company,
    r.channel,
    'Layer 2: Chi Phí Biến Đổi',
    '4. Vận chuyển',
    r.order_id as attribute_1,
    r.sku_code as attribute_2,
    r.ten_san_pham as attribute_3,
    r.promotion_type as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    ABS(SUM(r.phi_van_chuyen_thuc_te)) as amount,
    -- Tính % vận chuyển trên doanh thu kế toán
    CASE 
        WHEN b.total_revenue_base > 0 
        THEN ROUND((ABS(SUM(r.phi_van_chuyen_thuc_te)) / b.total_revenue_base) * 100, 2)
        ELSE 0 
    END as percent
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue` r
  LEFT JOIN doanh_thu_ke_toan_base b
    ON EXTRACT(YEAR FROM DATE(r.date_create)) = b.year 
    AND EXTRACT(MONTH FROM DATE(r.date_create)) = b.month 
    AND r.brand = b.brand 
    AND r.company = b.company
    AND r.channel = b.channel
    AND r.order_id = b.order_id 
    AND r.sku_code = b.sku_code
    AND r.promotion_type = b.promotion_type
  GROUP BY EXTRACT(YEAR FROM DATE(r.date_create)), EXTRACT(MONTH FROM DATE(r.date_create)), r.brand, r.company, r.channel, r.order_id, r.sku_code, r.ten_san_pham, r.promotion_type, b.total_revenue_base
  
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
    SUM(gia_von_total) as amount,
    0  as percent
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
    SUM(doanh_thu_ke_toan) * 0.02 as amount,
    0.02  as percent
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
    0 as amount,
    0  as percent
  FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
  GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel


  UNION ALL
  
  -- Tổng chi phí biến đổi
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
    total_chi_phi_bien_doi as amount,
    0  as percent
  FROM tong_chi_phi_bien_doi

    UNION ALL
  
  -- Layer 3: Chi Phí Biến Đổi
  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Lương NV BHTT (ONE7- ONE5)',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Lương NV BHTT (ONE7- ONE5)"

  UNION ALL


  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Lương QL BHTT (ONE5-ONE3)',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Lương QL BHTT (ONE5-ONE3)"

  UNION ALL


  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Lương NV MKT (ONE7-ONE3)',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Lương NV MKT (ONE7-ONE3)"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Lương NV CTV live',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Lương NV CTV live"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Chi phí MKT cố định',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Chi phí MKT cố định"

  UNION ALL
  
  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Tài Nguyên',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Tài Nguyên"

  UNION ALL 

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Phát triễn sản phẩm',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Phát triễn sản phẩm"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 3: Chuyển Đổi Gián Tiếp',
    'Tổng chi phí chuyển đổi gián tiếp',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Lương văn Phòng',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Lương văn Phòng"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Lương quản lý',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Lương quản lý"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Tuyển Dụng',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Tuyển Dụng"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Mặt bằng VP',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Mặt bằng VP"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Vận hành hành chính',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Vận hành hành chính"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Mặt bằng, vận hành kho',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Mặt bằng, vận hành kho"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Phí ngoại giao',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Phí ngoại giao"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Thuế ',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chuyển đổi gián tiếp" and layer2 ="Thuế "

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Chi phí BOD',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Chi phí BOD"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Sửa chữa mặt bằng',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Sửa chữa mặt bằng"

  UNION ALL

  SELECT 
    year,
    month,
    brand,
    company,
    "" as channel,
    'Layer 4: Chi Phí Quản Lý',
    'Khấu hao',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    total as amount,
    0 as percent
  FROM phi_co_dinh_percent
  where layer1 = "Chi phí quản lý" and layer2 ="Khấu hao"

  UNION ALL

  SELECT 
      year,
      month,
      brand,
      company,
      "" as channel,
      'Layer 4: Chi Phí Quản Lý',
      'Khấu hao cũ',
      "" as attribute_1,
      "" as attribute_2,
      "" as attribute_3,
      "" as attribute_4,
      "" as attribute_5,
      "" as attribute_6,
      "" as attribute_7,
      total as amount,
      0 as percent
    FROM phi_co_dinh_percent
    where layer1 = "Chi phí quản lý" and layer2 ="Khấu hao cũ"

  UNION ALL
  
  SELECT 
      year,
      month,
      brand,
      company,
      "" as channel,
      'Layer 4: Chi Phí Quản Lý',
      'Tổng chi phí quản lý',
      "" as attribute_1,
      "" as attribute_2,
      "" as attribute_3,
      "" as attribute_4,
      "" as attribute_5,
      "" as attribute_6,
      "" as attribute_7,
      total as amount,
      0 as percent
    FROM phi_co_dinh_percent
    where layer1 = "Chi phí quản lý"

  UNION ALL

  -- Thu nhập thuần
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    'Thu nhập thuần',
    'Thu nhập thuần',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    net_income as amount,
    net_profit_margin_pct as percent
  FROM ty_le_phan_tram
  WHERE net_income IS NOT NULL

  UNION ALL

  -- Thu nhập thuần
  SELECT 
    year,
    month,
    brand,
    company,
    channel,
    'Thu nhập thuần',
    'Thu nhập thuần2',
    "" as attribute_1,
    "" as attribute_2,
    "" as attribute_3,
    "" as attribute_4,
    "" as attribute_5,
    "" as attribute_6,
    "" as attribute_7,
    net_income as amount,
    net_profit_margin_pct as percent
  FROM ty_le_phan_tram_co_dinh
  WHERE net_income IS NOT NULL
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
  SUM(amount) as amount,
  -- Không dùng AVERAGE cho percentage, dùng logic tính lại
  CASE 
    WHEN layer_name = 'Layer 2: Chi Phí Biến Đổi' AND SUM(amount) > 0
    THEN MAX(percent)
    ELSE AVG(percent)
  END as percent
FROM base_data
WHERE amount IS NOT NULL 
  AND month >= 6 
  AND year >= 2025
GROUP BY year, month, brand, company, channel, layer_name, metric_name, attribute_1, attribute_2, attribute_3, attribute_4, attribute_5, attribute_6, attribute_7












-- -- CREATE VIEW `crypto-arcade-453509-i8.pl_reporting.v_pl_auto` AS
-- WITH 
-- -- CTE để tính tổng doanh thu theo SKU để phân bổ ads cost
-- sku_revenue_for_allocation AS (
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)) as year,
--     EXTRACT(MONTH FROM DATE(date_create)) as month,
--     brand,
--     company,
--     channel,
--     order_id,
--     sku_code,
--     ten_san_pham,
--     promotion_type,
--     SUM(doanh_thu_ke_toan) as sku_revenue
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY year, month, brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
-- ),
-- -- Thêm CTE mới để tính doanh thu theo đơn hàng:
-- order_revenue AS (
--   SELECT 
--     year,
--     month,
--     brand,
--     company,
--     channel,
--     order_id,
--     SUM(sku_revenue) as order_revenue
--   FROM sku_revenue_for_allocation
--   GROUP BY year, month, brand, company, channel, order_id
-- ),
-- -- CTE để tính doanh thu kế toán theo group chi tiết - SỬA LẠI
-- doanh_thu_ke_toan_base AS (
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)) as year,
--     EXTRACT(MONTH FROM DATE(date_create)) as month,
--     brand,
--     company,
--     channel,
--     order_id,
--     sku_code,
--     promotion_type,
--     SUM(doanh_thu_ke_toan) as total_revenue_base
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY year, month, brand, company, channel, order_id, sku_code, promotion_type
-- ),


-- -- CTE để tính tổng doanh thu theo brand, company, channel, month, year
-- total_revenue_by_group AS (
--   SELECT 
--     year,
--     month,
--     brand,
--     company,
--     channel,
--     SUM(sku_revenue) as total_group_revenue,
--     COUNT(DISTINCT order_id) AS total_order_count
--   FROM sku_revenue_for_allocation
--   GROUP BY year, month, brand, company, channel
-- ),


-- -- CTE để lấy tổng chi phí ads theo brand, company, channel, month, year
-- chi_phi_ads_total AS (
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_start)) as year,
--     EXTRACT(MONTH FROM DATE(date_start)) as month,
--     brand,
--     company,
--     channel,
--     SUM(chiPhiAds) as total_ads_cost
--   FROM `crypto-arcade-453509-i8.dtm.t3_ads_total_with_tkqc`
--   GROUP BY year, month, brand, company, channel
-- ),


-- sku_count_per_order AS (
--   SELECT 
--     year,
--     month,
--     brand,
--     company,
--     channel,
--     order_id,
--     COUNT(DISTINCT sku_code) as sku_count_in_order
--   FROM sku_revenue_for_allocation
--   GROUP BY year, month, brand, company, channel, order_id
-- ),
-- chi_phi_ads_allocated AS (
--   SELECT 
--     s.year,
--     s.month,
--     s.brand,
--     s.company,
--     s.channel,
--     s.order_id,
--     s.sku_code,
--     s.ten_san_pham,
--     s.promotion_type,
--     -- Phương thức 1: Theo doanh thu SKU
--     CASE 
--       WHEN t.total_group_revenue > 0 
--       THEN (s.sku_revenue / t.total_group_revenue) * a.total_ads_cost
--       ELSE 0 
--     END as allocated_ads_cost,
--     -- Phương thức 2: Chia đều cho đơn hàng, sau đó chia cho số SKU
--     CASE 
--       WHEN t.total_order_count > 0 AND sc.sku_count_in_order > 0
--       THEN (a.total_ads_cost / t.total_order_count) / sc.sku_count_in_order
--       ELSE 0
--     END as allocated_ads_cost_count_order
--   FROM sku_revenue_for_allocation s
--   LEFT JOIN total_revenue_by_group t
--     ON s.year = t.year 
--     AND s.month = t.month 
--     AND s.brand = t.brand 
--     AND s.company = t.company
--     AND s.channel = t.channel
--   LEFT JOIN order_revenue o
--     ON s.year = o.year 
--     AND s.month = o.month 
--     AND s.brand = o.brand 
--     AND s.company = o.company
--     AND s.channel = o.channel
--     AND s.order_id = o.order_id
--   LEFT JOIN chi_phi_ads_total a
--     ON s.year = a.year 
--     AND s.month = a.month 
--     AND s.brand = a.brand 
--     AND s.company = a.company
--     AND s.channel = a.channel
--   LEFT JOIN sku_count_per_order sc
--     ON s.year = sc.year 
--     AND s.month = sc.month 
--     AND s.brand = sc.brand 
--     AND s.company = sc.company
--     AND s.channel = sc.channel
--     AND s.order_id = sc.order_id
-- ),


-- -- CTE để tính tổng chi phí biến đổi từ các bảng khác nhau
-- chi_phi_revenue AS (
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)) as year,
--     EXTRACT(MONTH FROM DATE(date_create)) as month,
--     brand,
--     company,
--     channel,
--     SUM(phu_phi) as san_cost,
--     ABS(SUM(phi_van_chuyen_thuc_te)) as van_chuyen_cost,
--     SUM(gia_von_total) as gia_von
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY year, month, brand, company, channel
-- ),


-- -- Tính tổng chi phí biến đổi
-- tong_chi_phi_bien_doi AS (
--   SELECT 
--     COALESCE(a.year, r.year) as year,
--     COALESCE(a.month, r.month) as month,
--     COALESCE(a.brand, r.brand) as brand,
--     COALESCE(a.company, r.company) as company,
--     COALESCE(a.channel, r.channel) as channel,
--     COALESCE(SUM(a.total_ads_cost), 0) + COALESCE(r.san_cost, 0) + COALESCE(r.van_chuyen_cost, 0) + COALESCE(r.gia_von, 0) as total_chi_phi_bien_doi
--   FROM chi_phi_ads_total a
--   FULL OUTER JOIN chi_phi_revenue r
--     ON a.year = r.year 
--     AND a.month = r.month 
--     AND a.brand = r.brand 
--     AND a.company = r.company
--     AND a.channel = r.channel
--   GROUP BY year, month, brand, company, channel, r.san_cost, r.van_chuyen_cost, r.gia_von
-- ),


-- -- CTE tính tỷ lệ phần trăm
-- ty_le_phan_tram AS (
--   SELECT 
--     r.year,
--     r.month,
--     r.brand,
--     r.company,
--     r.channel,
--     r.total_group_revenue,
--     c.total_chi_phi_bien_doi,
--     r.total_group_revenue - c.total_chi_phi_bien_doi as net_income,
--     -- Tính Net Profit Margin %
--     CASE 
--       WHEN r.total_group_revenue > 0 
--       THEN ROUND(((r.total_group_revenue - c.total_chi_phi_bien_doi) / r.total_group_revenue) * 100, 2)
--       ELSE 0 
--     END as net_profit_margin_pct,
--     -- Tính Cost Ratio %
--     CASE 
--       WHEN r.total_group_revenue > 0 
--       THEN ROUND((c.total_chi_phi_bien_doi / r.total_group_revenue) * 100, 2)
--       ELSE 0 
--     END as cost_ratio_pct
--   FROM total_revenue_by_group r
--   LEFT JOIN tong_chi_phi_bien_doi c
--     ON r.year = c.year 
--     AND r.month = c.month 
--     AND r.brand = c.brand 
--     AND r.company = c.company
--     AND r.channel = c.channel
-- ),


-- base_data AS (
--   -- Layer 0: Doanh Thu bán hàng
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
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(gia_ban_daily_total) as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create_order)), EXTRACT(MONTH FROM DATE(date_create_order)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
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
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(doanh_thu_ke_toan) as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create_order)), EXTRACT(MONTH FROM DATE(date_create_order)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
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
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(gia_ban_daily_total) as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
--   UNION ALL

--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)) as year,
--     EXTRACT(MONTH FROM DATE(date_create)) as month,
--     brand,
--     company,
--     channel,
--     'Layer 1: Doanh Thu' as layer_name,
--     '2. Chiết khấu' as metric_name,
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(tien_chiet_khau_sp_tot) as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type
  
--   UNION ALL
  
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 1: Doanh Thu',
--     '3. Doanh thu kế toán',
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     status as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(doanh_thu_ke_toan) as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type, status


--   UNION ALL
  
--   SELECT 
--     EXTRACT(YEAR FROM DATE(date_create)),
--     EXTRACT(MONTH FROM DATE(date_create)),
--     brand,
--     company,
--     channel,
--     'Layer 1: Doanh Thu',
--     '3.1 Hoàn',
--     order_id as attribute_1,
--     sku_code as attribute_2,
--     ten_san_pham as attribute_3,
--     promotion_type as attribute_4,
--     status as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     CASE
--         WHEN status = "Đã hoàn"
--         THEN SUM(doanh_thu_ke_toan)
--         ELSE 0
--     END as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, order_id, sku_code, channel, ten_san_pham, promotion_type, status
  
--   UNION ALL
  
--   -- Layer 2: Chi Phí Biến Đổi với percentage - ĐÃ SỬA
--   -- 1. Giá vốn
--   SELECT 
--     EXTRACT(YEAR FROM DATE(r.date_create)) as year,
--     EXTRACT(MONTH FROM DATE(r.date_create)) as month,
--     r.brand,
--     r.company,
--     r.channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '1. Giá vốn',
--     r.order_id as attribute_1,
--     r.sku_code as attribute_2,
--     r.ten_san_pham as attribute_3,
--     r.promotion_type as attribute_4,
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(r.gia_von_total) as amount,
--     -- Tính % giá vốn trên doanh thu kế toán
--     CASE 
--         WHEN b.total_revenue_base > 0 
--         THEN ROUND((SUM(r.gia_von_total) / b.total_revenue_base) * 100, 2)
--         ELSE 0 
--     END as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue` r
--   LEFT JOIN doanh_thu_ke_toan_base b
--     ON EXTRACT(YEAR FROM DATE(r.date_create)) = b.year 
--     AND EXTRACT(MONTH FROM DATE(r.date_create)) = b.month 
--     AND r.brand = b.brand 
--     AND r.company = b.company
--     AND r.channel = b.channel
--     AND r.order_id = b.order_id 
--     AND r.sku_code = b.sku_code
--     AND r.promotion_type = b.promotion_type
--   GROUP BY EXTRACT(YEAR FROM DATE(r.date_create)), EXTRACT(MONTH FROM DATE(r.date_create)), r.brand, r.company, r.order_id, r.sku_code, r.channel, r.ten_san_pham, r.promotion_type, b.total_revenue_base


--   UNION ALL


--   -- 2. Ads (sum revenue) - ĐÃ SỬA
--   SELECT 
--     a.year,
--     a.month,
--     a.brand,
--     a.company,
--     a.channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '2. Ads (sum revenue)',
--     a.order_id as attribute_1,
--     a.sku_code as attribute_2,
--     a.ten_san_pham as attribute_3,
--     a.promotion_type as attribute_4,
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     a.allocated_ads_cost as amount,
--     -- Tính % ads cost trên doanh thu kế toán
--     CASE 
--         WHEN b.total_revenue_base > 0 
--         THEN ROUND((a.allocated_ads_cost / b.total_revenue_base) * 100, 2)
--         ELSE 0 
--     END as percent
--   FROM chi_phi_ads_allocated a
--   LEFT JOIN doanh_thu_ke_toan_base b
--     ON a.year = b.year 
--     AND a.month = b.month 
--     AND a.brand = b.brand 
--     AND a.company = b.company
--     AND a.channel = b.channel
--     AND a.order_id = b.order_id 
--     AND a.sku_code = b.sku_code
--     AND a.promotion_type = b.promotion_type


--   UNION ALL


--   -- 2b. Ads (count order) - ĐÃ SỬA
--   SELECT 
--     a.year,
--     a.month,
--     a.brand,
--     a.company,
--     a.channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '2b. Ads (count order)',
--     a.order_id as attribute_1,
--     a.sku_code as attribute_2,
--     a.ten_san_pham as attribute_3,
--     a.promotion_type as attribute_4,
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     a.allocated_ads_cost_count_order as amount,
--     -- Tính % ads cost (theo đơn) trên doanh thu kế toán
--     CASE 
--         WHEN b.total_revenue_base > 0 
--         THEN ROUND((a.allocated_ads_cost_count_order / b.total_revenue_base) * 100, 2)
--         ELSE 0 
--     END as percent
--   FROM chi_phi_ads_allocated a
--   LEFT JOIN doanh_thu_ke_toan_base b
--     ON a.year = b.year 
--     AND a.month = b.month 
--     AND a.brand = b.brand 
--     AND a.company = b.company
--     AND a.channel = b.channel
--     AND a.order_id = b.order_id 
--     AND a.sku_code = b.sku_code
--     AND a.promotion_type = b.promotion_type


--   UNION ALL


--   -- 3. Sàn - ĐÃ SỬA
--   SELECT 
--     EXTRACT(YEAR FROM DATE(r.date_create)) as year,
--     EXTRACT(MONTH FROM DATE(r.date_create)) as month,
--     r.brand,
--     r.company,
--     r.channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '3. Sàn',
--     r.order_id as attribute_1,
--     r.sku_code as attribute_2,
--     r.ten_san_pham as attribute_3,
--     r.promotion_type as attribute_4,
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(r.phu_phi) as amount,
--     -- Tính % phí sàn trên doanh thu kế toán
--     CASE 
--         WHEN b.total_revenue_base > 0 
--         THEN ROUND((SUM(r.phu_phi) / b.total_revenue_base) * 100, 2)
--         ELSE 0 
--     END as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue` r
--   LEFT JOIN doanh_thu_ke_toan_base b
--     ON EXTRACT(YEAR FROM DATE(r.date_create)) = b.year 
--     AND EXTRACT(MONTH FROM DATE(r.date_create)) = b.month 
--     AND r.brand = b.brand 
--     AND r.company = b.company
--     AND r.channel = b.channel
--     AND r.order_id = b.order_id 
--     AND r.sku_code = b.sku_code
--     AND r.promotion_type = b.promotion_type
--   GROUP BY EXTRACT(YEAR FROM DATE(r.date_create)), EXTRACT(MONTH FROM DATE(r.date_create)), r.brand, r.company, r.channel, r.order_id, r.sku_code, r.ten_san_pham, r.promotion_type, b.total_revenue_base


--   UNION ALL


--   -- 4. Vận chuyển - ĐÃ SỬA
--   SELECT 
--     EXTRACT(YEAR FROM DATE(r.date_create)) as year,
--     EXTRACT(MONTH FROM DATE(r.date_create)) as month,
--     r.brand,
--     r.company,
--     r.channel,
--     'Layer 2: Chi Phí Biến Đổi',
--     '4. Vận chuyển',
--     r.order_id as attribute_1,
--     r.sku_code as attribute_2,
--     r.ten_san_pham as attribute_3,
--     r.promotion_type as attribute_4,
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     ABS(SUM(r.phi_van_chuyen_thuc_te)) as amount,
--     -- Tính % vận chuyển trên doanh thu kế toán
--     CASE 
--         WHEN b.total_revenue_base > 0 
--         THEN ROUND((ABS(SUM(r.phi_van_chuyen_thuc_te)) / b.total_revenue_base) * 100, 2)
--         ELSE 0 
--     END as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue` r
--   LEFT JOIN doanh_thu_ke_toan_base b
--     ON EXTRACT(YEAR FROM DATE(r.date_create)) = b.year 
--     AND EXTRACT(MONTH FROM DATE(r.date_create)) = b.month 
--     AND r.brand = b.brand 
--     AND r.company = b.company
--     AND r.channel = b.channel
--     AND r.order_id = b.order_id 
--     AND r.sku_code = b.sku_code
--     AND r.promotion_type = b.promotion_type
--   GROUP BY EXTRACT(YEAR FROM DATE(r.date_create)), EXTRACT(MONTH FROM DATE(r.date_create)), r.brand, r.company, r.channel, r.order_id, r.sku_code, r.ten_san_pham, r.promotion_type, b.total_revenue_base
  
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
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(gia_von_total) as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   WHERE promotion_type = "Quà Tặng"
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel, order_id, sku_code, ten_san_pham, promotion_type
  
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
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     SUM(doanh_thu_ke_toan) * 0.02 as amount,
--     0.02  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel
  
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
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     0 as amount,
--     0  as percent
--   FROM `crypto-arcade-453509-i8.dtm.t3_pnl_revenue`
--   GROUP BY EXTRACT(YEAR FROM DATE(date_create)), EXTRACT(MONTH FROM DATE(date_create)), brand, company, channel


--   UNION ALL
  
--   -- Tổng chi phí biến đổi
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
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     total_chi_phi_bien_doi as amount,
--     0  as percent
--   FROM tong_chi_phi_bien_doi


--   UNION ALL
  
--   -- Thu nhập thuần
--   SELECT 
--     year,
--     month,
--     brand,
--     company,
--     channel,
--     'Thu nhập thuần',
--     'Thu nhập thuần',
--     "" as attribute_1,
--     "" as attribute_2,
--     "" as attribute_3,
--     "" as attribute_4,
--     "" as attribute_5,
--     "" as attribute_6,
--     "" as attribute_7,
--     net_income as amount,
--     net_profit_margin_pct as percent
--   FROM ty_le_phan_tram
--   WHERE net_income IS NOT NULL
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
--   attribute_5,
--   attribute_6,
--   attribute_7,
--   SUM(amount) as amount,
--   -- Không dùng AVERAGE cho percentage, dùng logic tính lại
--   CASE 
--     WHEN layer_name = 'Layer 2: Chi Phí Biến Đổi' AND SUM(amount) > 0
--     THEN MAX(percent)
--     ELSE AVG(percent)
--   END as percent
-- FROM base_data
-- WHERE amount IS NOT NULL 
--   AND month >= 6 
--   AND year >= 2025
-- GROUP BY year, month, brand, company, channel, layer_name, metric_name, attribute_1, attribute_2, attribute_3, attribute_4, attribute_5, attribute_6, attribute_7
