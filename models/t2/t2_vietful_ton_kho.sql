WITH current_inventory AS (
  -- Lấy tồn kho khả dụng hiện tại (ngày gần nhất)
  SELECT 
    warehouse_code,
    sku,
    partner_sku,
    available_qty,
    brand
  FROM {{ref("t1_vietful_product_inventory")}}
  WHERE date_record = (SELECT MAX(date_record) FROM {{ref("t1_vietful_product_inventory")}})
),

daily_outbound AS (
  -- Tính số lượng xuất kho theo ngày
  SELECT 
    t.warehouse_code,
    d.sku,
    d.partnerSKU,
    d.categoryName,
    t.brand,
    DATE(t.shipped_date) as ship_date,
    SUM(d.packedQty) as daily_qty
  FROM {{ref("t1_vietful_xuatkho_total")}} t
  JOIN {{ref("t1_vietful_xuat_kho_details")}} d ON t.or_code = d.or_code
  WHERE t.shipped_date IS NOT NULL
    AND DATE(t.shipped_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1,2,3,4,5,6
),

monthly_outbound AS (
  -- Số lượng xuất trong tháng hiện tại
  SELECT 
    warehouse_code,
    sku,
    partnerSKU,
    categoryName,
    brand,
    SUM(daily_qty) as monthly_qty
  FROM daily_outbound
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
    SUM(daily_qty) as today_qty
  FROM daily_outbound
  WHERE ship_date = CURRENT_DATE()
  GROUP BY 1,2,3
),

mov3_calculation AS (
  -- MOV3: Trung bình 3 ngày gần nhất (không tính hôm nay)
  SELECT 
    warehouse_code,
    sku,
    partnerSKU,
    AVG(daily_qty) as mov3
  FROM daily_outbound
  WHERE ship_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) 
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY 1,2,3
),

mov7_calculation AS (
  -- MOV7: Trung bình 7 ngày gần nhất (không tính hôm nay)
  SELECT 
    warehouse_code,
    sku,
    partnerSKU,
    AVG(daily_qty) as mov7
  FROM daily_outbound
  WHERE ship_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY 1,2,3
)
, a as(
-- Kết hợp tất cả dữ liệu
SELECT 
  i.warehouse_code as KHO,
  i.sku as SKU,
  i.partner_sku as SKU_DOI_TAC,
  COALESCE(m.categoryName, 'Không có tên') as TEN_SAN_PHAM,
  COALESCE(m.monthly_qty, 0) as SL_XUAT_TRONG_THANG,
  COALESCE(t.today_qty, 0) as SL_XUAT_HOM_NAY,
  COALESCE(mov3.mov3, 0) as MOV3,
  COALESCE(mov7.mov7, 0) as MOV7,
  i.available_qty as TON_KHO_KHA_DUNG,
  CASE 
    WHEN COALESCE(mov3.mov3, 0) > 0 
    THEN ROUND(i.available_qty / mov3.mov3, 2) 
    ELSE NULL 
  END as DOH_3,
  CASE 
    WHEN COALESCE(mov7.mov7, 0) > 0 
    THEN ROUND(i.available_qty / mov7.mov7, 2) 
    ELSE NULL 
  END as DOH_7,
  7 as thoi_gian_nhap_kho,
  i.brand
FROM current_inventory i
LEFT JOIN monthly_outbound m ON i.warehouse_code = m.warehouse_code 
  AND i.sku = m.sku AND i.partner_sku = m.partnerSKU
LEFT JOIN today_outbound t ON i.warehouse_code = t.warehouse_code 
  AND i.sku = t.sku AND i.partner_sku = t.partnerSKU
LEFT JOIN mov3_calculation mov3 ON i.warehouse_code = mov3.warehouse_code 
  AND i.sku = mov3.sku AND i.partner_sku = mov3.partnerSKU
LEFT JOIN mov7_calculation mov7 ON i.warehouse_code = mov7.warehouse_code 
  AND i.sku = mov7.sku AND i.partner_sku = mov7.partnerSKU
ORDER BY i.warehouse_code, i.sku)

select * from a
