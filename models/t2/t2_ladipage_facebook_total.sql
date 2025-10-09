WITH marketer_fix AS (
  SELECT 
    od.*,
    CASE
      WHEN od.marketer IS NULL THEN 'NULL'
      WHEN JSON_VALUE(od.marketer, '$.name') NOT IN (
        SELECT DISTINCT marketer_name FROM {{ref("t1_marketer_facebook_total")}}
      ) THEN 'NULL'
      ELSE JSON_VALUE(od.marketer, '$.name')
    END AS marketer_fixed
  FROM {{ref("t1_pancake_pos_order_total")}} AS od
),
a AS (
  SELECT 
    DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) AS date_insert,
    'Facebook' AS channel,
    mar.company,
    od.brand,
    od.brand AS brand_lv1,
    '' AS loai_khach_hang,
    mar.marketing_name AS staff_name,
    mar.ma_nhan_vien AS id_staff,
    mar.manager AS manager_name,
    mar.ma_quan_ly AS ma_quan_ly,
    SUM(od.total_price_after_sub_discount) AS doanhThuLadi,
    NULL AS doanh_so_moi,
    NULL AS doanh_so_cu
  FROM marketer_fix AS od
  LEFT JOIN {{ref("t1_marketer_facebook_total")}} AS mar
    ON od.marketer_fixed = mar.marketer_name
    AND DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) BETWEEN mar.start_date AND mar.end_date
    AND od.brand = mar.brand
  WHERE mar.company = 'One5'
    AND od.order_sources_name IN ('Facebook', 'Ladipage Facebook', 'Webcake','Instagram','Zalo')
    AND LOWER(od.note) NOT LIKE '%hủy%'
    AND od.status_name NOT IN ('removed','canceled')
  GROUP BY 
    DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)),
    mar.company,
    od.brand,
    loai_khach_hang,
    mar.marketing_name,
    mar.ma_nhan_vien,
    mar.manager,
    mar.ma_quan_ly
),
b as (
select * from a 

union all

SELECT
  DATE(a.ngay_chot_don) AS date_insert,
  a.channel,
  'Max Eagle' AS company,
  a.brand,
  a.brand_lv1,
  a.loai_khach_hang,
  a.marketing_name AS staff_name,
  a.ma_nhan_vien AS id_staff,
  a.manager AS manager_name,
  a.ma_quan_ly AS ma_quan_ly,
  SUM(a.doanh_so) AS doanhThuLadi,
  SUM(a.doanh_so_moi) AS doanh_so_moi,
  SUM(a.doanh_so_cu) AS doanh_so_cu
FROM {{ref("t2_mapping_sandbox_pushsale_toa")}} a
GROUP BY 
  DATE(a.ngay_chot_don),
  a.channel,
  a.brand,
  a.brand_lv1,
  a.loai_khach_hang,
  a.marketing_name,
  a.ma_nhan_vien,
  a.manager,
  a.ma_quan_ly
)

select * from b