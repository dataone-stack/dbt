WITH current_inventory AS (
  SELECT 
    warehouse_code,
    sku,
    partner_sku,
    available_qty,
    brand,
    date_record
  FROM dtm.t1_vietful_product_inventory
  WHERE condition_type_code = 'NEW'   -- chỉ lấy hàng là NEW
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY warehouse_code, sku, partner_sku, brand -- Thêm brand
    ORDER BY date_record DESC
  ) = 1
),
-- Tạo danh sách tất cả các ngày cần tính
date_range AS (
  SELECT 
    date_add(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY), INTERVAL day_offset DAY) as date_value
  FROM 
    UNNEST(GENERATE_ARRAY(0, 30)) as day_offset
),

-- Tạo danh sách tất cả sản phẩm và ngày
product_date_matrix AS (
  SELECT DISTINCT
    t.warehouse_code,
    d.sku,
    d.partnerSKU,
    p.product_name,
    t.brand,
    dr.date_value
  FROM dtm.t1_vietful_xuatkho_total t
  JOIN dtm.t1_vietful_xuat_kho_details d ON t.or_code = d.or_code
  left JOIN {{ref("t1_vietful_product_total")}} p ON p.sku = d.sku and t.brand = p.brand
  CROSS JOIN date_range dr
  where p.sku is not null
),

daily_outbound_raw AS (
  -- Tính số lượng xuất kho theo ngày (chỉ những ngày có xuất)
  SELECT 
    t.warehouse_code,
    d.sku,
    d.partnerSKU,
    p.product_name,
    t.brand,
    DATE(t.shipped_date) as ship_date,
    SUM(d.packedQty) as daily_qty
  FROM dtm.t1_vietful_xuatkho_total t
  JOIN dtm.t1_vietful_xuat_kho_details d ON t.or_code = d.or_code
  left JOIN {{ref("t1_vietful_product_total")}} p ON p.sku = d.sku  and t.brand = p.brand
  WHERE t.shipped_date IS NOT NULL
    AND DATE(t.shipped_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1,2,3,4,5,6
),

daily_outbound_complete AS (
  -- Kết hợp matrix với dữ liệu thực tế, điền 0 cho những ngày không xuất
  SELECT 
    pm.warehouse_code,
    pm.sku,
    pm.partnerSKU,
    pm.product_name,
    pm.brand,
    pm.date_value as ship_date,
    COALESCE(do.daily_qty, 0) as daily_qty
  FROM product_date_matrix pm
  LEFT JOIN daily_outbound_raw do ON pm.warehouse_code = do.warehouse_code
    AND pm.sku = do.sku 
    AND pm.partnerSKU = do.partnerSKU
    AND pm.date_value = do.ship_date
),

monthly_outbound AS (
  -- Số lượng xuất trong tháng hiện tại
  SELECT 
    warehouse_code,
    sku,
    partnerSKU,
    product_name,
    brand,
    SUM(daily_qty) as monthly_qty
  FROM daily_outbound_complete
  WHERE EXTRACT(MONTH FROM ship_date) = EXTRACT(MONTH FROM CURRENT_DATE())
    AND EXTRACT(YEAR FROM ship_date) = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY 1,2,3,4,5
),

today_outbound AS (
  -- Số lượng xuất hôm nay
  SELECT 
    warehouse_code,
    sku,
    partnerSKU,
    brand,
    SUM(daily_qty) as today_qty
  FROM daily_outbound_complete
  WHERE ship_date = CURRENT_DATE()
  GROUP BY 1,2,3,4
),

mov3_calculation AS (
  -- MOV3: Trung bình 3 ngày gần nhất (không tính hôm nay) - bao gồm cả ngày không xuất
  SELECT 
    warehouse_code,
    sku,
    partnerSKU,
    brand,
    AVG(daily_qty) as mov3
  FROM daily_outbound_complete
  WHERE ship_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) 
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY 1,2,3,4
),

mov7_calculation AS (
  -- MOV7: Trung bình 7 ngày gần nhất (không tính hôm nay) - bao gồm cả ngày không xuất
  SELECT 
    warehouse_code,
    sku,
    partnerSKU,
    brand,
    AVG(daily_qty) as mov7
  FROM daily_outbound_complete
  WHERE ship_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY 1,2,3,4
),

all_products AS (
  SELECT DISTINCT 
    warehouse_code,
    sku,
    partner_sku,
    brand
  FROM (
    -- Từ current_inventory
    SELECT 
      warehouse_code,
      sku,
      partner_sku,
      brand
    FROM dtm.t1_vietful_product_inventory
    WHERE date_record = (SELECT MAX(date_record) FROM dtm.t1_vietful_product_inventory)
    
    UNION ALL
    
    -- Từ monthly_outbound  
    SELECT
      warehouse_code,
      sku,
      partnerSKU as partner_sku,
      brand
    FROM monthly_outbound
  )
),

a AS (
  SELECT 
    ap.warehouse_code as KHO,
    ap.sku as SKU,
    ap.partner_sku as SKU_DOI_TAC,
    COALESCE(m.product_name, 'Không có tên') as TEN_SAN_PHAM,
    COALESCE(m.monthly_qty, 0) as SL_XUAT_TRONG_THANG,
    COALESCE(t.today_qty, 0) as SL_XUAT_HOM_NAY,
    COALESCE(mov3.mov3, 0) as MOV3,
    COALESCE(mov7.mov7, 0) as MOV7,
    COALESCE(i.available_qty, 0) as TON_KHO_KHA_DUNG,
    CASE 
      WHEN COALESCE(mov3.mov3, 0) > 0 
      THEN ROUND(COALESCE(i.available_qty, 0) / mov3.mov3, 2) 
      ELSE NULL 
    END as DOH_3,
    CASE 
      WHEN COALESCE(mov7.mov7, 0) > 0 
      THEN ROUND(COALESCE(i.available_qty, 0) / mov7.mov7, 2) 
      ELSE NULL 
    END as DOH_7,
    7 as thoi_gian_nhap_kho,
    ap.brand,
    bang_gia.brand_lv1 as bg_brand_lv1,
    bang_gia.brand as bg_brand,
    
    -- Thêm các status fields
    CASE 
      WHEN COALESCE(mov3.mov3, 0) = 0 THEN 'Không có xuất kho 3 ngày'
      WHEN COALESCE(i.available_qty, 0) = 0 THEN 'Hết hàng'
      WHEN (COALESCE(i.available_qty, 0) / mov3.mov3) <= 3 THEN 'Sắp hết hàng'
      WHEN (COALESCE(i.available_qty, 0) / mov3.mov3) > 7 THEN 'An toàn'
      ELSE 'Bình thường'
    END as STATUS_DOH3,
    
    CASE 
      WHEN COALESCE(mov7.mov7, 0) = 0 THEN 'Không có xuất kho 7 ngày'
      WHEN COALESCE(i.available_qty, 0) = 0 THEN 'Hết hàng'
      WHEN (COALESCE(i.available_qty, 0) / mov7.mov7) <= 3 THEN 'Sắp hết hàng'
      WHEN (COALESCE(i.available_qty, 0) / mov7.mov7) > 7 THEN 'An toàn'
      ELSE 'Bình thường'
    END as STATUS_DOH7,
    
    -- Status tổng hợp (ưu tiên DOH7)
    CASE 
      WHEN COALESCE(mov7.mov7, 0) = 0 THEN 'Không có xuất kho'
      WHEN COALESCE(i.available_qty, 0) = 0 THEN 'Hết hàng'
      WHEN (COALESCE(i.available_qty, 0) / mov7.mov7) <= 3 THEN 'Sắp hết hàng'
      WHEN (COALESCE(i.available_qty, 0) / mov7.mov7) > 7 THEN 'An toàn'
      ELSE 'Bình thường'
    END as STATUS_TONG_HOP,
    
    -- Mức độ ưu tiên (cho sorting/filtering)
    CASE 
      WHEN COALESCE(i.available_qty, 0) = 0 THEN 1
      WHEN COALESCE(mov7.mov7, 0) = 0 THEN 2
      WHEN (COALESCE(i.available_qty, 0) / mov7.mov7) <= 3 THEN 3
      WHEN (COALESCE(i.available_qty, 0) / mov7.mov7) <= 7 THEN 4
      ELSE 5
    END as MUC_DO_UU_TIEN,
    
    -- Flag fields cho Power BI filtering
    CASE WHEN COALESCE(mov3.mov3, 0) = 0 THEN 1 ELSE 0 END as FLAG_KHONG_XUAT_3_NGAY,
    CASE WHEN COALESCE(mov7.mov7, 0) = 0 THEN 1 ELSE 0 END as FLAG_KHONG_XUAT_7_NGAY,
    CASE WHEN COALESCE(i.available_qty, 0) = 0 THEN 1 ELSE 0 END as FLAG_HET_HANG,
    CASE WHEN COALESCE(i.available_qty, 0) > 0 AND (COALESCE(i.available_qty, 0) / NULLIF(mov7.mov7, 0)) <= 3 THEN 1 ELSE 0 END as FLAG_SAP_HET_HANG,
    CASE WHEN (COALESCE(i.available_qty, 0) / NULLIF(mov7.mov7, 0)) > 7 THEN 1 ELSE 0 END as FLAG_AN_TOAN
    
  FROM all_products ap
  LEFT JOIN current_inventory i ON ap.warehouse_code = i.warehouse_code 
    AND ap.sku = i.sku AND ap.partner_sku = i.partner_sku AND ap.brand = i.brand
  LEFT JOIN monthly_outbound m ON ap.warehouse_code = m.warehouse_code 
    AND ap.sku = m.sku AND ap.partner_sku = m.partnerSKU AND ap.brand = m.brand
  LEFT JOIN today_outbound t ON ap.warehouse_code = t.warehouse_code 
    AND ap.sku = t.sku AND ap.partner_sku = t.partnerSKU AND ap.brand = t.brand
  LEFT JOIN mov3_calculation mov3 ON ap.warehouse_code = mov3.warehouse_code 
    AND ap.sku = mov3.sku AND ap.partner_sku = mov3.partnerSKU AND ap.brand = mov3.brand
  LEFT JOIN mov7_calculation mov7 ON ap.warehouse_code = mov7.warehouse_code 
    AND ap.sku = mov7.sku  AND ap.partner_sku = mov7.partnerSKU AND ap.brand = mov7.brand
  LEFT JOIN {{ref("t1_bang_gia_san_pham")}} bang_gia ON bang_gia.ma_sku = ap.partner_sku
  ORDER BY MUC_DO_UU_TIEN, ap.warehouse_code, ap.sku
)
SELECT * FROM a