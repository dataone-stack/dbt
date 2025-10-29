WITH a AS (
  SELECT
    DATE(ngay_data_ve) AS ngay_tao_contact,
    sale_name,
    sale_user_name,
    manager,
    ma_quan_ly,
    COUNT(DISTINCT IF(LOWER(sale_user_name) LIKE '%sale%' , ma_don_so, NULL)) AS so_lead_sale,

   
    COUNT(DISTINCT IF(LOWER(sale_user_name) LIKE '%cskh%', ma_don_so, NULL)) AS so_lead_cskh

  FROM {{ref("t2_sandbox_order_lines_toa")}}
  WHERE ngay_data_ve IS NOT NULL
  GROUP BY DATE(ngay_data_ve), sale_name, sale_user_name,manager, ma_quan_ly
)
--  select * from a WHERE sale_user_name = 
--   'team9me.cskh01'
--   AND ngay_tao_contact BETWEEN DATE '2025-10-01' AND DATE '2025-10-05'
,

b AS (
  SELECT 
    DATE(ngay_chot_don) AS ngay_chot_don,
    sale_name,
    sale_user_name,
    manager,
    ma_quan_ly,
    COUNT(DISTINCT ma_don_so) AS so_sale,
    SUM(IFNULL(doanh_so_cu,0)) AS doanh_so_cu,
    SUM(IFNULL(doanh_so_moi,0)) AS doanh_so_moi
  FROM {{ref("t2_sandbox_order_lines_toa")}}
  WHERE ngay_chot_don IS NOT NULL and order_confirm_name = 'Chốt đơn'
  GROUP BY DATE(ngay_chot_don), sale_name, sale_user_name,manager, ma_quan_ly
-- select * from b WHERE sale_user_name = 
--    'team9me.sale04'
--    AND ngay_chot_don BETWEEN DATE '2025-10-01' AND DATE '2025-10-05'
),

c AS (
  SELECT
    a.ngay_tao_contact,
    a.sale_name,
    a.sale_user_name,
    a.manager,
    a.ma_quan_ly,
    (a.so_lead_sale + a.so_lead_cskh) AS so_lead,
  
    COALESCE(b.so_sale, 0) AS so_sale,
    COALESCE(b.doanh_so_cu, 0) AS doanh_so_cu,
    COALESCE(b.doanh_so_moi, 0) AS doanh_so_moi
  FROM a
  LEFT JOIN b
    ON a.ngay_tao_contact = b.ngay_chot_don
   AND trim(a.sale_user_name) = trim(b.sale_user_name)
)

SELECT
  ngay_tao_contact,
  sale_name,
  sale_user_name,
  manager,
  ma_quan_ly,
  SUM(so_lead) AS so_lead,
  SUM(so_sale) AS so_sale,
  SUM(doanh_so_cu) AS doanh_so_cu,
  SUM(doanh_so_moi) AS doanh_so_moi
FROM c

GROUP BY ngay_tao_contact,sale_name, sale_user_name,manager,ma_quan_ly
ORDER BY so_lead desc
