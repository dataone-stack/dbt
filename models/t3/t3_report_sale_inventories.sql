-- models/marts/inventory/dm_inventory_performance.sql

WITH date_spine AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM ngay_tao_don) AS year,
    EXTRACT(MONTH FROM ngay_tao_don) AS month,
    DATE_TRUNC(DATE(ngay_tao_don), MONTH) AS period_start_date,
    LAST_DAY(DATE(ngay_tao_don), MONTH) AS period_end_date,
    'MONTH' AS period_type
  FROM `crypto-arcade-453509-i8`.`dtm`.`t3_revenue_all_channel`
  WHERE ngay_tao_don IS NOT NULL
),

warehouse_dim AS (
  SELECT DISTINCT
    warehouse_code,
    warehouse_code as warehouse_name
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_vietful_product_inventory`
  WHERE condition_type_code = 'NEW'
),

inventory_snapshot AS (
  SELECT
    partner_sku as sku,
    brand,
    warehouse_code,
    date_record,
    EXTRACT(YEAR FROM date_record) AS year,
    EXTRACT(MONTH FROM date_record) AS month,
    available_qty
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_vietful_product_inventory`
  WHERE condition_type_code = 'NEW'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
      partner_sku,
      warehouse_code,
      DATE_TRUNC(DATE(date_record), MONTH)
    ORDER BY date_record DESC
  ) = 1
),

current_inventory AS (
  SELECT
    partner_sku as sku,
    brand,
    warehouse_code,
    date_record AS current_date,
    available_qty AS ton_kho_hien_tai
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_vietful_product_inventory`
  WHERE condition_type_code = 'NEW'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY partner_sku, warehouse_code
    ORDER BY date_record DESC
  ) = 1
),

inventory_beginning AS (
  SELECT
    sku,
    warehouse_code,
    EXTRACT(YEAR FROM DATE_ADD(date_record, INTERVAL 1 MONTH)) AS year,
    EXTRACT(MONTH FROM DATE_ADD(date_record, INTERVAL 1 MONTH)) AS month,
    SUM(available_qty) AS ton_kho_dau_ky
  FROM inventory_snapshot
  GROUP BY sku, warehouse_code, year, month
),

inventory_ending AS (
  SELECT
    sku,
    warehouse_code,
    year,
    month,
    SUM(available_qty) AS ton_kho_cuoi_ky
  FROM inventory_snapshot
  GROUP BY sku, warehouse_code, year, month
),

plan_data AS (
  SELECT
    year,
    month,
    brand,
    sku,
    product_name,
    SUM(quantity) AS ke_hoach_ban
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_plan_bussiness_monthly_by_sku`
  GROUP BY year, month, brand, sku, product_name
),

actual_sales AS (
  SELECT
    EXTRACT(YEAR FROM ngay_tao_don) AS year,
    EXTRACT(MONTH FROM ngay_tao_don) AS month,
    brand,
    sku_code AS sku,
    SUM(so_luong) AS thuc_te_ban
  FROM `crypto-arcade-453509-i8`.`dtm`.`t3_revenue_all_channel`
  WHERE status_dang_don NOT IN ('Đã hủy', 'Trả hàng/Hoàn tiền')
    AND ngay_tao_don IS NOT NULL
  GROUP BY year, month, brand, sku_code
),

product_lead_time AS (
  SELECT
    brand,
    sku,
    product_name,
    -- ✅ MẶC ĐỊNH 30 NGÀY NẾU KHÔNG CÓ LEAD TIME
    COALESCE(lead_time, 30) AS lead_time
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_product_lead_time`
),

expected_arrivals AS (
  SELECT
    sku,
    brand,
    MIN(PARSE_DATE('%Y-%m-%d', expected_return_date)) AS next_arrival_date,
    SUM(order_quantity - COALESCE(quantity_received, 0)) AS incoming_qty
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_plan_expected_arrival_of_goods`
  WHERE status != 'Đã nhận đủ'
    AND SAFE.PARSE_DATE('%Y-%m-%d', expected_return_date) IS NOT NULL
    AND SAFE.PARSE_DATE('%Y-%m-%d', expected_return_date) >= CURRENT_DATE()
  GROUP BY sku, brand
)
-- Final datamart
SELECT
  ds.year,
  ds.month,
  ds.period_start_date,
  ds.period_end_date,
  ds.period_type,
  
  COALESCE(p.brand, a.brand) AS brand,
  COALESCE(p.sku, a.sku, ib.sku, ie.sku) AS sku,
  p.product_name,
  
  COALESCE(ib.warehouse_code, ie.warehouse_code, ci.warehouse_code) AS warehouse_code,
  wd.warehouse_name AS ten_kho,
  
  -- ============ TÍNH SỐ NGÀY THỰC TẾ ============
  CASE 
    -- Nếu là tháng hiện tại → dùng ngày hiện tại
    WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
         AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
    THEN EXTRACT(DAY FROM CURRENT_DATE())
    
    -- Nếu không → dùng tổng số ngày trong tháng đó
    ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
  END AS actual_days_in_period,
  
  COALESCE(ib.ton_kho_dau_ky, 0) AS ton_kho,
  COALESCE(p.ke_hoach_ban, 0) AS ke_hoach,
  COALESCE(a.thuc_te_ban, 0) AS thuc_te,
  COALESCE(ib.ton_kho_dau_ky, 0) - COALESCE(a.thuc_te_ban, 0) AS con_lai,
  
  CASE 
    WHEN COALESCE(p.ke_hoach_ban, 0) > 0 
    THEN ROUND((COALESCE(a.thuc_te_ban, 0) / p.ke_hoach_ban), 2)
    ELSE 0 
  END AS ke_hoach_dat_duoc,
  
  COALESCE(ie.ton_kho_cuoi_ky, 0) AS ton_kho_cuoi_ky,
  COALESCE(ci.ton_kho_hien_tai, 0) AS ton_kho_hien_tai,
  ci.current_date AS ngay_cap_nhat_ton_kho,
  
  CURRENT_TIMESTAMP() AS last_updated_at,

  COALESCE(plt.lead_time, 30) AS lead_time_days,
  ea.next_arrival_date AS expected_arrival_date,
  ea.incoming_qty AS incoming_quantity,

  -- ============ CẬP NHẬT CÁC CÔNG THỨC SỬ DỤNG ACTUAL_DAYS ============
  
  -- 1. So sánh DOI vs Lead Time (CẬP NHẬT)
  CASE 
    WHEN COALESCE(a.thuc_te_ban, 0) > 0
    THEN ROUND(
      (COALESCE(ci.ton_kho_hien_tai, 0) / 
        (a.thuc_te_ban / 
          CASE 
            WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                 AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
            THEN EXTRACT(DAY FROM CURRENT_DATE())
            ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
          END
        )
      ) - COALESCE(plt.lead_time, 30),
      1
    )
    ELSE NULL
  END AS doi_vs_leadtime_gap,

  -- 2. Ngày dự kiến hết hàng (CẬP NHẬT)
  CASE 
    WHEN COALESCE(a.thuc_te_ban, 0) > 0 
         AND COALESCE(ci.ton_kho_hien_tai, 0) > 0
    THEN DATE_ADD(
      ci.current_date, 
      INTERVAL CAST(ROUND(
        COALESCE(ci.ton_kho_hien_tai, 0) / 
        (a.thuc_te_ban / 
          CASE 
            WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                 AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
            THEN EXTRACT(DAY FROM CURRENT_DATE())
            ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
          END
        )
      ) AS INT64) DAY
    )
    ELSE NULL
  END AS predicted_stockout_date,

  -- 3. Số ngày còn lại trước khi hết hàng (CẬP NHẬT)
  CASE 
    WHEN COALESCE(a.thuc_te_ban, 0) > 0 
         AND COALESCE(ci.ton_kho_hien_tai, 0) > 0
    THEN CAST(ROUND(
      COALESCE(ci.ton_kho_hien_tai, 0) / 
      (a.thuc_te_ban / 
        CASE 
          WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
               AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
          THEN EXTRACT(DAY FROM CURRENT_DATE())
          ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
        END
      )
    ) AS INT64)
    ELSE 999
  END AS days_until_stockout,

  -- 4. Tồn kho sau khi hàng về
  COALESCE(ci.ton_kho_hien_tai, 0) + COALESCE(ea.incoming_qty, 0) AS ton_kho_sau_khi_hang_ve,

  -- 5. STOCKOUT RISK STATUS (CẬP NHẬT - Tính daily_sales_rate một lần)
  CASE
    WHEN COALESCE(a.thuc_te_ban, 0) = 0 
    THEN '⚪ KHÔNG CÓ DATA BÁN - Không đánh giá được'
    
    WHEN ea.next_arrival_date IS NULL THEN
      CASE
        WHEN COALESCE(ci.ton_kho_hien_tai, 0) = 0 
        THEN '🔴 HẾT HÀNG + KHÔNG CÓ ĐƠN NHẬP - Cần lên đơn GẤP'
        
        WHEN COALESCE(ci.ton_kho_hien_tai, 0) >= 
          (a.thuc_te_ban / 
            CASE 
              WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                   AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
              THEN EXTRACT(DAY FROM CURRENT_DATE())
              ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
            END
          ) * COALESCE(plt.lead_time, 30) * 2
        THEN '🟢 TỒN KHO ĐỦ - Đủ dùng >60 ngày, chưa cần nhập'
        
        WHEN COALESCE(ci.ton_kho_hien_tai, 0) >= 
          (a.thuc_te_ban / 
            CASE 
              WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                   AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
              THEN EXTRACT(DAY FROM CURRENT_DATE())
              ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
            END
          ) * COALESCE(plt.lead_time, 30)
        THEN '🟠 TỒN KHO VỪA ĐỦ - Đủ dùng 30-60 ngày, nên lên đơn ngay'
        
        WHEN COALESCE(ci.ton_kho_hien_tai, 0) < 
          (a.thuc_te_ban / 
            CASE 
              WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                   AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
              THEN EXTRACT(DAY FROM CURRENT_DATE())
              ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
            END
          ) * COALESCE(plt.lead_time, 30)
        THEN '🔴 TỒN KHO THIẾU - Đủ dùng <30 ngày, cần lên đơn GẤP'
        
        ELSE '⚪ KHÔNG XÁC ĐỊNH'
      END
    
    WHEN DATE_ADD(
           ci.current_date, 
           INTERVAL CAST(ROUND(
             COALESCE(ci.ton_kho_hien_tai, 0) / 
             (a.thuc_te_ban / 
               CASE 
                 WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                      AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
                 THEN EXTRACT(DAY FROM CURRENT_DATE())
                 ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
               END
             )
           ) AS INT64) DAY
         ) < ea.next_arrival_date
    THEN '🔴 SẼ HẾT HÀNG TRƯỚC KHI NHẬP VỀ - Nguy cơ cao'
    
    WHEN DATE_ADD(
           ci.current_date, 
           INTERVAL CAST(ROUND(
             COALESCE(ci.ton_kho_hien_tai, 0) / 
             (a.thuc_te_ban / 
               CASE 
                 WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                      AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
                 THEN EXTRACT(DAY FROM CURRENT_DATE())
                 ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
               END
             )
           ) AS INT64) DAY
         ) BETWEEN ea.next_arrival_date 
                   AND DATE_ADD(ea.next_arrival_date, INTERVAL 3 DAY)
    THEN '🟠 SẮP HẾT HÀNG KHI NHẬP VỀ - Cần theo dõi sát'
    
    WHEN DATE_ADD(
           ci.current_date, 
           INTERVAL CAST(ROUND(
             COALESCE(ci.ton_kho_hien_tai, 0) / 
             (a.thuc_te_ban / 
               CASE 
                 WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                      AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
                 THEN EXTRACT(DAY FROM CURRENT_DATE())
                 ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
               END
             )
           ) AS INT64) DAY
         ) >= DATE_ADD(ea.next_arrival_date, INTERVAL 3 DAY)
    THEN '🟢 ĐỦ HÀNG ĐẾN KHI NHẬP VỀ - An toàn'
    
    ELSE '⚪ KHÔNG XÁC ĐỊNH'
  END AS stockout_risk_status,

  -- 6. Buffer days (CẬP NHẬT)
  CASE 
    WHEN COALESCE(a.thuc_te_ban, 0) > 0 
         AND ea.next_arrival_date IS NOT NULL
    THEN DATE_DIFF(
      ea.next_arrival_date,
      DATE_ADD(
        ci.current_date, 
        INTERVAL CAST(ROUND(
          COALESCE(ci.ton_kho_hien_tai, 0) / 
          (a.thuc_te_ban / 
            CASE 
              WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                   AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
              THEN EXTRACT(DAY FROM CURRENT_DATE())
              ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
            END
          )
        ) AS INT64) DAY
      ),
      DAY
    )
    ELSE NULL
  END AS buffer_days_before_arrival,

  -- 8. SALES PRIORITY STATUS (Trạng thái ưu tiên bán hàng)
  CASE
    -- Không có data bán hoặc hết hàng
    WHEN COALESCE(a.thuc_te_ban, 0) = 0 
         OR COALESCE(ci.ton_kho_hien_tai, 0) = 0
    THEN '🔴 ƯU TIÊN CAO'
    
    -- Tồn kho cao (>60 ngày) - CẦN ĐẨY MẠNH BÁN
    WHEN CAST(ROUND(
      COALESCE(ci.ton_kho_hien_tai, 0) / 
      (a.thuc_te_ban / 
        CASE 
          WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
               AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
          THEN EXTRACT(DAY FROM CURRENT_DATE())
          ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
        END
      )
    ) AS INT64) > 60
    THEN '🔴 ƯU TIÊN CAO'
    
    -- Tồn kho vừa (30-60 ngày) - BÁN BÌNH THƯỜNG
    WHEN CAST(ROUND(
      COALESCE(ci.ton_kho_hien_tai, 0) / 
      (a.thuc_te_ban / 
        CASE 
          WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
               AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
          THEN EXTRACT(DAY FROM CURRENT_DATE())
          ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
        END
      )
    ) AS INT64) BETWEEN 30 AND 60
    THEN '🟠 ƯU TIÊN TRUNG BÌNH'
    
    -- Tồn kho thấp (<30 ngày) - KHÔNG NÊN ĐẨY MẠNH
    WHEN CAST(ROUND(
      COALESCE(ci.ton_kho_hien_tai, 0) / 
      (a.thuc_te_ban / 
        CASE 
          WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
               AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
          THEN EXTRACT(DAY FROM CURRENT_DATE())
          ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
        END
      )
    ) AS INT64) < 30
    THEN '🟢 ƯU TIÊN THẤP'
    
    ELSE '⚪ KHÔNG XÁC ĐỊNH'
  END AS sales_priority_status,


  -- 9. PROCUREMENT PRIORITY STATUS (Trạng thái ưu tiên nhập hàng)
  CASE
    -- Không có data bán - không đánh giá được
    WHEN COALESCE(a.thuc_te_ban, 0) = 0 
    THEN '⚪ Chưa bán được sản phẩm nào'
    
    -- 🔴 CẤP BÁCH - Cần lên đơn GẤP
    WHEN COALESCE(ci.ton_kho_hien_tai, 0) = 0
    THEN '🔴 CẤP BÁCH'
    
    WHEN ea.next_arrival_date IS NULL 
         AND CAST(ROUND(
           COALESCE(ci.ton_kho_hien_tai, 0) / 
           (a.thuc_te_ban / 
             CASE 
               WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                    AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
               THEN EXTRACT(DAY FROM CURRENT_DATE())
               ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
             END
           )
         ) AS INT64) < COALESCE(plt.lead_time, 30)
    THEN '🔴 CẤP BÁCH'
    
    WHEN DATE_ADD(
           ci.current_date, 
           INTERVAL CAST(ROUND(
             COALESCE(ci.ton_kho_hien_tai, 0) / 
             (a.thuc_te_ban / 
               CASE 
                 WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                      AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
                 THEN EXTRACT(DAY FROM CURRENT_DATE())
                 ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
               END
             )
           ) AS INT64) DAY
         ) < ea.next_arrival_date
    THEN '🔴 CẤP BÁCH'
    
    -- 🟠 ƯU TIÊN CAO - Nên lên đơn trong tuần
    WHEN ea.next_arrival_date IS NULL 
         AND CAST(ROUND(
           COALESCE(ci.ton_kho_hien_tai, 0) / 
           (a.thuc_te_ban / 
             CASE 
               WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                    AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
               THEN EXTRACT(DAY FROM CURRENT_DATE())
               ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
             END
           )
         ) AS INT64) BETWEEN COALESCE(plt.lead_time, 30) 
                              AND COALESCE(plt.lead_time, 30) * 1.5
    THEN '🟠 ƯU TIÊN CAO'
    
    WHEN DATE_ADD(
           ci.current_date, 
           INTERVAL CAST(ROUND(
             COALESCE(ci.ton_kho_hien_tai, 0) / 
             (a.thuc_te_ban / 
               CASE 
                 WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                      AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
                 THEN EXTRACT(DAY FROM CURRENT_DATE())
                 ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
               END
             )
           ) AS INT64) DAY
         ) BETWEEN ea.next_arrival_date 
                   AND DATE_ADD(ea.next_arrival_date, INTERVAL 7 DAY)
    THEN '🟠 ƯU TIÊN CAO'
    
    -- 🟢 ƯU TIÊN TRUNG BÌNH - Theo dõi
    WHEN ea.next_arrival_date IS NULL 
         AND CAST(ROUND(
           COALESCE(ci.ton_kho_hien_tai, 0) / 
           (a.thuc_te_ban / 
             CASE 
               WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                    AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
               THEN EXTRACT(DAY FROM CURRENT_DATE())
               ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
             END
           )
         ) AS INT64) BETWEEN COALESCE(plt.lead_time, 30) * 1.5 
                              AND COALESCE(plt.lead_time, 30) * 2
    THEN '🟢 ƯU TIÊN TRUNG BÌNH'
    
    WHEN DATE_ADD(
           ci.current_date, 
           INTERVAL CAST(ROUND(
             COALESCE(ci.ton_kho_hien_tai, 0) / 
             (a.thuc_te_ban / 
               CASE 
                 WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                      AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
                 THEN EXTRACT(DAY FROM CURRENT_DATE())
                 ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
               END
             )
           ) AS INT64) DAY
         ) BETWEEN DATE_ADD(ea.next_arrival_date, INTERVAL 7 DAY)
                   AND DATE_ADD(ea.next_arrival_date, INTERVAL 14 DAY)
    THEN '🟢 ƯU TIÊN TRUNG BÌNH'
    
    -- ⚪ KHÔNG CẦN NHẬP - Đủ hàng
    WHEN CAST(ROUND(
      COALESCE(ci.ton_kho_hien_tai, 0) / 
      (a.thuc_te_ban / 
        CASE 
          WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
               AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
          THEN EXTRACT(DAY FROM CURRENT_DATE())
          ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
        END
      )
    ) AS INT64) >= COALESCE(plt.lead_time, 30) * 2
    THEN '⚪ KHÔNG CẦN NHẬP'
    
    WHEN DATE_ADD(
           ci.current_date, 
           INTERVAL CAST(ROUND(
             COALESCE(ci.ton_kho_hien_tai, 0) / 
             (a.thuc_te_ban / 
               CASE 
                 WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
                      AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
                 THEN EXTRACT(DAY FROM CURRENT_DATE())
                 ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
               END
             )
           ) AS INT64) DAY
         ) > DATE_ADD(ea.next_arrival_date, INTERVAL 14 DAY)
    THEN '⚪ KHÔNG CẦN NHẬP'
    
    ELSE '⚪ KHÔNG XÁC ĐỊNH'
  END AS procurement_priority_status,


  -- 7. Days of Inventory (DOI) - giữ nguyên
  CASE 
    WHEN COALESCE(a.thuc_te_ban, 0) > 0 
    THEN ROUND(
      COALESCE(ci.ton_kho_hien_tai, 0) / 
      (a.thuc_te_ban / 
        CASE 
          WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
               AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
          THEN EXTRACT(DAY FROM CURRENT_DATE())
          ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
        END
      ),
      1
    )
    ELSE 999 
  END AS days_of_inventory

FROM date_spine ds

LEFT JOIN plan_data p
  ON ds.year = p.year
  AND ds.month = p.month

LEFT JOIN actual_sales a
  ON ds.year = a.year
  AND ds.month = a.month
  AND p.sku = a.sku
  AND p.brand = a.brand

LEFT JOIN inventory_beginning ib
  ON ds.year = ib.year
  AND ds.month = ib.month
  AND COALESCE(p.sku, a.sku) = ib.sku

LEFT JOIN inventory_ending ie
  ON ds.year = ie.year
  AND ds.month = ie.month
  AND COALESCE(p.sku, a.sku, ib.sku) = ie.sku
  AND COALESCE(ib.warehouse_code, '') = COALESCE(ie.warehouse_code, '')

LEFT JOIN current_inventory ci
  ON COALESCE(p.sku, a.sku, ib.sku, ie.sku) = ci.sku
  AND COALESCE(ib.warehouse_code, ie.warehouse_code, '') = COALESCE(ci.warehouse_code, '')

LEFT JOIN warehouse_dim wd
  ON COALESCE(ib.warehouse_code, ie.warehouse_code, ci.warehouse_code) = wd.warehouse_code

LEFT JOIN product_lead_time plt
  ON COALESCE(p.sku, a.sku, ib.sku, ie.sku) = plt.sku
  AND COALESCE(p.brand, a.brand) = plt.brand

LEFT JOIN expected_arrivals ea
  ON COALESCE(p.sku, a.sku, ib.sku, ie.sku) = ea.sku
  AND COALESCE(p.brand, a.brand) = ea.brand

WHERE COALESCE(p.sku, a.sku, ib.sku, ie.sku) IS NOT NULL
