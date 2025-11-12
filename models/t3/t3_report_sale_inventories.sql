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

brand_warehouse_mapping AS (
  SELECT 'ME' AS company, 'AMS SLIM' AS brand, 'UME' AS warehouse_code UNION ALL
  SELECT 'ME', 'An Cung', 'UME' UNION ALL
  SELECT 'ME', 'BE20', 'UME' UNION ALL
  SELECT 'ME', 'Cà phê gừng', 'UME' UNION ALL
  SELECT 'ME', 'Cà Phê Mầm Xôi', 'UME' UNION ALL
  SELECT 'ME', 'Chaching Beauty', 'UME' UNION ALL
  SELECT 'ME', 'Chanh tây', 'UME' UNION ALL
  SELECT 'ME', 'Dr Diva', 'UME' UNION ALL
  SELECT 'ME', 'LYB Cosmetics', 'UME' UNION ALL
  SELECT 'ME', 'UME', 'UME' UNION ALL
  SELECT 'One5', 'An Cung', 'UME' UNION ALL
  SELECT 'One5', 'Chaching', 'Chaching' UNION ALL
  SELECT 'One5', 'Chaching Beauty', 'Chaching' UNION ALL
  SELECT 'One5', 'LYB', 'LYB' UNION ALL
  SELECT 'One5', 'UME', 'UME'
),

inventory_snapshot AS (
  SELECT
    inv.partner_sku as sku,
    inv.brand,
    bwm.company,
    COALESCE(bwm.warehouse_code, inv.warehouse_code) AS warehouse_code,
    inv.date_record,
    EXTRACT(YEAR FROM inv.date_record) AS year,
    EXTRACT(MONTH FROM inv.date_record) AS month,
    inv.available_qty
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_vietful_product_inventory` inv
  LEFT JOIN brand_warehouse_mapping bwm
    ON inv.brand = bwm.brand
  WHERE inv.condition_type_code = 'NEW'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
      inv.partner_sku,
      COALESCE(bwm.warehouse_code, inv.warehouse_code),
      bwm.company,
      DATE_TRUNC(DATE(inv.date_record), MONTH)
    ORDER BY inv.date_record DESC
  ) = 1
),

current_inventory AS (
  SELECT
    inv.partner_sku as sku,
    inv.brand,
    bwm.company,
    COALESCE(bwm.warehouse_code, inv.warehouse_code) AS warehouse_code,
    inv.date_record AS current_date,
    inv.available_qty AS ton_kho_hien_tai
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_vietful_product_inventory` inv
  LEFT JOIN brand_warehouse_mapping bwm
    ON inv.brand = bwm.brand
  WHERE inv.condition_type_code = 'NEW'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY inv.partner_sku, COALESCE(bwm.warehouse_code, inv.warehouse_code), bwm.company
    ORDER BY inv.date_record DESC
  ) = 1
),

inventory_beginning AS (
  SELECT
    sku,
    warehouse_code,
    company,
    EXTRACT(YEAR FROM DATE_ADD(date_record, INTERVAL 1 MONTH)) AS year,
    EXTRACT(MONTH FROM DATE_ADD(date_record, INTERVAL 1 MONTH)) AS month,
    SUM(available_qty) AS ton_kho_dau_ky
  FROM inventory_snapshot
  GROUP BY sku, warehouse_code, company, year, month
),

inventory_ending AS (
  SELECT
    sku,
    warehouse_code,
    company,
    year,
    month,
    SUM(available_qty) AS ton_kho_cuoi_ky
  FROM inventory_snapshot
  GROUP BY sku, warehouse_code, company, year, month
),

plan_data AS (
  SELECT
    p.year,
    p.month,
    p.brand,
    p.sku,
    p.product_name,
    bwm.company,
    COALESCE(bwm.warehouse_code, p.brand) AS warehouse_code,
    SUM(p.quantity) AS ke_hoach_ban
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_plan_bussiness_monthly_by_sku` p
  LEFT JOIN brand_warehouse_mapping bwm
    ON p.brand = bwm.brand
  GROUP BY p.year, p.month, p.brand, p.sku, p.product_name, bwm.company, warehouse_code
),

actual_sales AS (
  SELECT
    EXTRACT(YEAR FROM s.ngay_tao_don) AS year,
    EXTRACT(MONTH FROM s.ngay_tao_don) AS month,
    s.brand,
    s.company,
    s.sku_code AS sku,
    COALESCE(bwm.warehouse_code, s.brand) AS warehouse_code,
    SUM(s.so_luong) AS thuc_te_ban
  FROM `crypto-arcade-453509-i8`.`dtm`.`t3_revenue_all_channel` s
  LEFT JOIN brand_warehouse_mapping bwm
    ON s.brand = bwm.brand
    AND s.company = bwm.company
  WHERE s.status_dang_don NOT IN ('Đã hủy', 'Trả hàng/Hoàn tiền')
    AND s.ngay_tao_don IS NOT NULL
  GROUP BY year, month, s.brand, s.sku_code, s.company, warehouse_code
),

product_lead_time AS (
  SELECT
    brand,
    sku,
    product_name,
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
),

warehouse_doi_comparison AS (
  SELECT
    ci.sku,
    ci.warehouse_code,
    ci.ton_kho_hien_tai,
    a.thuc_te_ban,
    plt.lead_time,
    CASE 
      WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
           AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
      THEN EXTRACT(DAY FROM CURRENT_DATE())
      ELSE EXTRACT(DAY FROM LAST_DAY(ds.period_start_date))
    END AS actual_days_in_period,
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
    END AS doi
  FROM current_inventory ci
  LEFT JOIN (
    SELECT DISTINCT year, month, period_start_date 
    FROM date_spine 
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE())
      AND month = EXTRACT(MONTH FROM CURRENT_DATE())
  ) ds ON TRUE
  LEFT JOIN actual_sales a
    ON ci.sku = a.sku
    AND ci.warehouse_code = a.warehouse_code
    AND ds.year = a.year
    AND ds.month = a.month
  LEFT JOIN product_lead_time plt
    ON ci.sku = plt.sku
),

-- ✅ FIX: Chỉ lấy 1 kho nguồn tốt nhất cho mỗi kho thiếu
warehouse_transfer_alert AS (
  SELECT
    sku,
    kho_thieu,
    kho_du,
    doi_kho_thieu,
    doi_kho_du,
    ton_kho_thieu,
    ton_kho_du,
    lead_time,
    so_luong_nen_chuyen
  FROM (
    SELECT
      w1.sku,
      w1.warehouse_code AS kho_thieu,
      w2.warehouse_code AS kho_du,
      w1.doi AS doi_kho_thieu,
      w2.doi AS doi_kho_du,
      w1.ton_kho_hien_tai AS ton_kho_thieu,
      w2.ton_kho_hien_tai AS ton_kho_du,
      w1.lead_time,
      GREATEST(0, CAST(
        ((w1.lead_time * 1.5) - w1.doi) * 
        (w1.thuc_te_ban / w1.actual_days_in_period)
      AS INT64)) AS so_luong_nen_chuyen,
      -- ✅ Ưu tiên kho có DOI cao nhất và tồn kho nhiều nhất
      ROW_NUMBER() OVER (
        PARTITION BY w1.sku, w1.warehouse_code 
        ORDER BY w2.doi DESC, w2.ton_kho_hien_tai DESC
      ) AS rank_priority
    FROM warehouse_doi_comparison w1
    INNER JOIN warehouse_doi_comparison w2
      ON w1.sku = w2.sku
      AND w1.warehouse_code != w2.warehouse_code
    WHERE w1.doi < w1.lead_time * 1.5
      AND w2.doi > w2.lead_time * 1.5
      AND w1.thuc_te_ban > 0
  )
  WHERE rank_priority = 1  -- ✅ CHỈ LẤY KHO NGUỒN TỐT NHẤT
)

-- Final datamart
SELECT
  ds.year,
  ds.month,
  ds.period_start_date,
  ds.period_end_date,
  ds.period_type,
  
  COALESCE(a.company, p.company, ib.company, ie.company, ci.company) AS company,
  COALESCE(p.brand, a.brand) AS brand,
  COALESCE(p.sku, a.sku, ib.sku, ie.sku) AS sku,
  p.product_name,
  
  COALESCE(p.warehouse_code, a.warehouse_code, ib.warehouse_code, ie.warehouse_code, ci.warehouse_code) AS ten_kho,
  
  CASE 
    WHEN ds.year = EXTRACT(YEAR FROM CURRENT_DATE())
         AND ds.month = EXTRACT(MONTH FROM CURRENT_DATE())
    THEN EXTRACT(DAY FROM CURRENT_DATE())
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

  COALESCE(ci.ton_kho_hien_tai, 0) + COALESCE(ea.incoming_qty, 0) AS ton_kho_sau_khi_hang_ve,

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

  CASE
    WHEN COALESCE(a.thuc_te_ban, 0) = 0 
         OR COALESCE(ci.ton_kho_hien_tai, 0) = 0
    THEN '🔴 ƯU TIÊN CAO'
    
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

  CASE
    WHEN COALESCE(a.thuc_te_ban, 0) = 0 
    THEN '⚪ Chưa bán được sản phẩm nào'
    
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
  END AS days_of_inventory,

  -- Cảnh báo chuyển hàng
  wta.kho_du AS chuyen_hang_tu_kho,
  wta.ton_kho_du AS ton_kho_kho_nguon,
  wta.doi_kho_du AS doi_kho_nguon,
  wta.so_luong_nen_chuyen,
  
  CASE 
    WHEN wta.kho_du IS NOT NULL 
    THEN CONCAT(
      '🔄 NÊN CHUYỂN ', 
      wta.so_luong_nen_chuyen, 
      ' sản phẩm từ kho ', 
      wta.kho_du, 
      ' (DOI: ', wta.doi_kho_du, ' ngày) ',
      'sang kho ', 
      wta.kho_thieu,
      ' (DOI: ', wta.doi_kho_thieu, ' ngày)'
    )
    ELSE NULL
  END AS canh_bao_chuyen_hang

FROM date_spine ds

LEFT JOIN plan_data p
  ON ds.year = p.year
  AND ds.month = p.month

LEFT JOIN actual_sales a
  ON ds.year = a.year
  AND ds.month = a.month
  AND p.sku = a.sku
  AND p.brand = a.brand
  AND p.warehouse_code = a.warehouse_code
  AND COALESCE(p.company, '') = COALESCE(a.company, '')

LEFT JOIN inventory_beginning ib
  ON ds.year = ib.year
  AND ds.month = ib.month
  AND COALESCE(p.sku, a.sku) = ib.sku
  AND COALESCE(p.warehouse_code, a.warehouse_code) = ib.warehouse_code
  AND COALESCE(p.company, a.company, '') = COALESCE(ib.company, '')

LEFT JOIN inventory_ending ie
  ON ds.year = ie.year
  AND ds.month = ie.month
  AND COALESCE(p.sku, a.sku, ib.sku) = ie.sku
  AND COALESCE(p.warehouse_code, a.warehouse_code, ib.warehouse_code) = ie.warehouse_code
  AND COALESCE(p.company, a.company, ib.company, '') = COALESCE(ie.company, '')

LEFT JOIN current_inventory ci
  ON COALESCE(p.sku, a.sku, ib.sku, ie.sku) = ci.sku
  AND COALESCE(p.warehouse_code, a.warehouse_code, ib.warehouse_code, ie.warehouse_code) = ci.warehouse_code
  AND COALESCE(p.company, a.company, ib.company, ie.company, '') = COALESCE(ci.company, '')

LEFT JOIN product_lead_time plt
  ON COALESCE(p.sku, a.sku, ib.sku, ie.sku) = plt.sku
  AND COALESCE(p.brand, a.brand) = plt.brand

LEFT JOIN expected_arrivals ea
  ON COALESCE(p.sku, a.sku, ib.sku, ie.sku) = ea.sku
  AND COALESCE(p.brand, a.brand) = ea.brand

LEFT JOIN warehouse_transfer_alert wta
  ON COALESCE(p.sku, a.sku, ib.sku, ie.sku) = wta.sku
  AND COALESCE(p.warehouse_code, a.warehouse_code, ib.warehouse_code, ie.warehouse_code, ci.warehouse_code) = wta.kho_thieu

WHERE COALESCE(p.sku, a.sku, ib.sku, ie.sku) IS NOT NULL
 --and ib.sku = "CHA-153"
 