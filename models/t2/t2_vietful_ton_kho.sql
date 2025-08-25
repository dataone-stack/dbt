WITH base_data AS (
  SELECT
    sku,
    product_name,
    date_record,
    condition_type_code,
    daily_physical_inventory,
    daily_available_inventory,
    daily_total_pending_in,
    daily_total_pending_out,

    -- Thay đổi tồn kho vật lý ngày trước
    daily_physical_inventory - LAG(daily_physical_inventory) OVER (PARTITION BY sku ORDER BY date_record) AS daily_inventory_change
  FROM dtm.t2_vietful_ton_kho
),

avg_calc AS (
  SELECT
    *,
    -- Trung bình động 3 ngày tồn kho
    AVG(daily_physical_inventory) OVER (PARTITION BY sku ORDER BY date_record ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MOV3_inventory,

    -- Trung bình động 7 ngày tồn kho
    AVG(daily_physical_inventory) OVER (PARTITION BY sku ORDER BY date_record ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOV7_inventory,

    -- Trung bình thay đổi tồn kho 7 ngày
    AVG(daily_inventory_change) OVER (PARTITION BY sku ORDER BY date_record ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_inventory_change_7days
  FROM base_data
),

predicted_out_of_stock AS (
  SELECT
    *,
    CASE
      WHEN avg_inventory_change_7days < 0 THEN daily_physical_inventory / ABS(avg_inventory_change_7days)
      ELSE NULL
    END AS predicted_days_to_stockout,

    CASE
      WHEN avg_inventory_change_7days < 0 THEN DATE_ADD(date_record, INTERVAL CAST(daily_physical_inventory / ABS(avg_inventory_change_7days) AS INT64) DAY)
      ELSE NULL
    END AS predicted_out_of_stock_date
  FROM avg_calc
),a as(

SELECT
  sku,
  product_name,
  date_record,
  condition_type_code,
  daily_physical_inventory,
  daily_available_inventory,
  daily_total_pending_in,
  daily_total_pending_out,
  daily_inventory_change,
  MOV3_inventory,
  MOV7_inventory,
  avg_inventory_change_7days,
  predicted_days_to_stockout,
  predicted_out_of_stock_date,
  
  CASE
    WHEN daily_physical_inventory = 0 THEN 5
    WHEN avg_inventory_change_7days >= 0 THEN 1
    WHEN predicted_days_to_stockout <= 3 THEN 5
    WHEN predicted_days_to_stockout <= 7 THEN 4
    WHEN predicted_days_to_stockout <= 14 THEN 3
    WHEN predicted_days_to_stockout <= 30 THEN 2
    ELSE 1
  END AS stock_risk_level,
  
  CASE
    WHEN daily_physical_inventory = 0 THEN 'HET_HANG'
    WHEN avg_inventory_change_7days >= 0 THEN 'BINH_THUONG'
    WHEN predicted_days_to_stockout <= 7 THEN 'CANH_BAO_CAO'
    WHEN predicted_days_to_stockout <= 14 THEN 'CANH_BAO_TRUNG_BINH'
    ELSE 'BINH_THUONG'
  END AS stock_status

FROM predicted_out_of_stock
ORDER BY sku, date_record)

select * from a -- where sku = "8936188880122"








-- WITH date_range AS (
--   -- Tạo danh sách các ngày từ 31/07/2025 đến hiện tại
--   SELECT date_val
--   FROM UNNEST(GENERATE_DATE_ARRAY(
--     DATE('2025-07-31'),
--     CURRENT_DATE('Asia/Ho_Chi_Minh')
--   )) AS date_val
-- ),

-- all_skus AS (
--   -- Lấy tất cả SKU có dữ liệu từ ngày 31/07
--   SELECT DISTINCT sku
--   FROM {{ ref('t1_vietful_product_inventory') }}
--   WHERE date_record >= DATE('2025-07-31')
-- ),

-- product_conditions AS (
--   -- Lấy tất cả các condition_type_code có trong dữ liệu
--   SELECT DISTINCT 
--     sku,
--     condition_type_code
--   FROM {{ ref('t1_vietful_product_inventory') }}
--   WHERE date_record >= DATE('2025-07-31')
-- ),

-- date_condition_combinations AS (
--   -- Tạo tất cả các kết hợp ngày x SKU x condition_type_code
--   SELECT 
--     dr.date_val as date_record,
--     pc.sku,
--     pc.condition_type_code
--   FROM date_range dr
--   CROSS JOIN product_conditions pc
-- ),

-- daily_inventory AS (
--   SELECT 
--     sku,
--     date_record,
--     warehouse_code,
--     condition_type_code,
--     SUM(physical_qty) AS total_physical_qty,
--     SUM(available_qty) AS total_available_qty,
--     SUM(pending_in_qty) AS total_pending_in,
--     SUM(pending_out_qty) AS total_pending_out,
--     MAX(last_updated_date) AS last_update
--   FROM {{ ref('t1_vietful_product_inventory') }}
--   WHERE date_record >= DATE('2025-07-31')
--   GROUP BY sku, date_record, warehouse_code, condition_type_code
-- ),

-- aggregated_daily AS (
--   SELECT 
--     sku,
--     date_record,
--     condition_type_code,
--     SUM(total_physical_qty) AS daily_physical_inventory,
--     SUM(total_available_qty) AS daily_available_inventory,
--     SUM(total_pending_in) AS daily_total_pending_in,
--     SUM(total_pending_out) AS daily_total_pending_out
--   FROM daily_inventory
--   GROUP BY sku, date_record, condition_type_code
-- ),

-- filled_inventory AS (
--   SELECT 
--     dcc.sku,
--     dcc.date_record,
--     dcc.condition_type_code,
--     ad.daily_physical_inventory,
--     ad.daily_available_inventory,
--     COALESCE(ad.daily_total_pending_in, 0) AS daily_total_pending_in,
--     COALESCE(ad.daily_total_pending_out, 0) AS daily_total_pending_out,
--     -- Sử dụng LAST_VALUE để lấy giá trị gần nhất cho từng SKU và condition
--     LAST_VALUE(ad.daily_physical_inventory IGNORE NULLS) 
--       OVER (PARTITION BY dcc.sku, dcc.condition_type_code ORDER BY dcc.date_record 
--             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS carried_forward_physical,
--     LAST_VALUE(ad.daily_available_inventory IGNORE NULLS) 
--       OVER (PARTITION BY dcc.sku, dcc.condition_type_code ORDER BY dcc.date_record 
--             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS carried_forward_available
--   FROM date_condition_combinations dcc
--   LEFT JOIN aggregated_daily ad 
--     ON dcc.sku = ad.sku 
--     AND dcc.date_record = ad.date_record 
--     AND dcc.condition_type_code = ad.condition_type_code
-- )

-- SELECT 
--   fi.sku,
--   pro.product_name,
--   fi.date_record,
--   fi.condition_type_code,
--   COALESCE(fi.daily_physical_inventory, fi.carried_forward_physical, 0) AS daily_physical_inventory,
--   COALESCE(fi.daily_available_inventory, fi.carried_forward_available, 0) AS daily_available_inventory,
--   fi.daily_total_pending_in,
--   fi.daily_total_pending_out,
--   CASE 
--     WHEN fi.daily_physical_inventory IS NOT NULL THEN 'Actual'
--     WHEN fi.carried_forward_physical IS NOT NULL THEN 'Carried Forward'
--     ELSE 'No Data'
--   END AS data_source
-- FROM filled_inventory fi
-- LEFT JOIN {{ ref('t1_vietful_product_total') }} pro
--   ON pro.sku = fi.sku
-- ORDER BY fi.sku, fi.condition_type_code, fi.date_record DESC
