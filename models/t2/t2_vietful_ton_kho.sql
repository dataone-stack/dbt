{# SELECT 
  inve.warehouse_code,
  inve.partner_sku,
  inve.sku,
  pro.product_name,
  inve.unit_code,
  inve.condition_type_code,
  inve.date_record,
  inve.physical_qty,
  inve.available_qty,
  inve.pending_in_qty,
  inve.pending_out_qty,
  inve.freeze_qty
FROM 
  {{ ref('t1_vietful_product_inventory') }} inve
  LEFT JOIN {{ ref('t1_vietful_product_total') }} pro
  ON pro.sku = inve.sku #}

WITH date_range AS (
  -- Tạo danh sách các ngày từ 31/07/2025 đến hiện tại
  SELECT date_val
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE('2025-07-31'),
    CURRENT_DATE('Asia/Ho_Chi_Minh')
  )) AS date_val
),

all_skus AS (
  -- Lấy tất cả SKU có dữ liệu từ ngày 31/07
  SELECT DISTINCT sku
  FROM {{ ref('t1_vietful_product_inventory') }}
  WHERE date_record >= DATE('2025-07-31')
),

product_conditions AS (
  -- Lấy tất cả các condition_type_code có trong dữ liệu
  SELECT DISTINCT 
    sku,
    condition_type_code
  FROM {{ ref('t1_vietful_product_inventory') }}
  WHERE date_record >= DATE('2025-07-31')
),

date_condition_combinations AS (
  -- Tạo tất cả các kết hợp ngày x SKU x condition_type_code
  SELECT 
    dr.date_val as date_record,
    pc.sku,
    pc.condition_type_code
  FROM date_range dr
  CROSS JOIN product_conditions pc
),

daily_inventory AS (
  SELECT 
    sku,
    date_record,
    warehouse_code,
    condition_type_code,
    SUM(physical_qty) AS total_physical_qty,
    SUM(available_qty) AS total_available_qty,
    SUM(pending_in_qty) AS total_pending_in,
    SUM(pending_out_qty) AS total_pending_out,
    MAX(last_updated_date) AS last_update
  FROM {{ ref('t1_vietful_product_inventory') }}
  WHERE date_record >= DATE('2025-07-31')
  GROUP BY sku, date_record, warehouse_code, condition_type_code
),

aggregated_daily AS (
  SELECT 
    sku,
    date_record,
    condition_type_code,
    SUM(total_physical_qty) AS daily_physical_inventory,
    SUM(total_available_qty) AS daily_available_inventory,
    SUM(total_pending_in) AS daily_total_pending_in,
    SUM(total_pending_out) AS daily_total_pending_out
  FROM daily_inventory
  GROUP BY sku, date_record, condition_type_code
),

filled_inventory AS (
  SELECT 
    dcc.sku,
    dcc.date_record,
    dcc.condition_type_code,
    ad.daily_physical_inventory,
    ad.daily_available_inventory,
    COALESCE(ad.daily_total_pending_in, 0) AS daily_total_pending_in,
    COALESCE(ad.daily_total_pending_out, 0) AS daily_total_pending_out,
    -- Sử dụng LAST_VALUE để lấy giá trị gần nhất cho từng SKU và condition
    LAST_VALUE(ad.daily_physical_inventory IGNORE NULLS) 
      OVER (PARTITION BY dcc.sku, dcc.condition_type_code ORDER BY dcc.date_record 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS carried_forward_physical,
    LAST_VALUE(ad.daily_available_inventory IGNORE NULLS) 
      OVER (PARTITION BY dcc.sku, dcc.condition_type_code ORDER BY dcc.date_record 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS carried_forward_available
  FROM date_condition_combinations dcc
  LEFT JOIN aggregated_daily ad 
    ON dcc.sku = ad.sku 
    AND dcc.date_record = ad.date_record 
    AND dcc.condition_type_code = ad.condition_type_code
)

SELECT 
  fi.sku,
  pro.product_name,
  fi.date_record,
  fi.condition_type_code,
  COALESCE(fi.daily_physical_inventory, fi.carried_forward_physical, 0) AS daily_physical_inventory,
  COALESCE(fi.daily_available_inventory, fi.carried_forward_available, 0) AS daily_available_inventory,
  fi.daily_total_pending_in,
  fi.daily_total_pending_out,
  CASE 
    WHEN fi.daily_physical_inventory IS NOT NULL THEN 'Actual'
    WHEN fi.carried_forward_physical IS NOT NULL THEN 'Carried Forward'
    ELSE 'No Data'
  END AS data_source
FROM filled_inventory fi
LEFT JOIN {{ ref('t1_vietful_product_total') }} pro
  ON pro.sku = fi.sku
ORDER BY fi.sku, fi.condition_type_code, fi.date_record DESC
