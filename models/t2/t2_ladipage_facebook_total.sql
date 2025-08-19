SELECT 
  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) AS date_insert,
  'Facebook' AS channel,
  mar.company,
  od.brand,
  '' as loai_khach_hang,-- không có dữ liệu để NULL
  mar.marketing_name as staff_name,
  mar.ma_nhan_vien AS id_staff,
  mar.manager as manager_name,
  mar.ma_quan_ly AS ma_quan_ly,
  SUM(od.total_price_after_sub_discount) AS doanhThuLadi,
  NULL AS doanh_so_moi,   -- không có dữ liệu để NULL
  NULL AS doanh_so_cu     -- không có dữ liệu để NULL
FROM {{ref('t1_pancake_pos_order_total')}} AS od
LEFT JOIN {{ref("t1_marketer_facebook_total")}} AS mar
  ON JSON_VALUE(od.marketer, '$.name') = mar.marketer_name
WHERE od.marketer IS NOT NULL 
  AND mar.company = 'One5'
  AND od.order_sources_name IN ('Facebook', 'Ladipage Facebook', 'Webcake')
  AND (od.brand != 'UME' OR (od.brand = 'UME' AND od.status_name NOT IN ('new', 'removed')))
GROUP BY 
  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)),
  mar.company,
  od.brand,
  loai_khach_hang,
  mar.marketing_name,
  mar.ma_nhan_vien,
  mar.manager,
  mar.ma_quan_ly

UNION ALL

SELECT
  DATE(a.ngay_chot_don) AS date_insert,
  a.channel,
  'Max Eagle' AS company,
  a.brand,
  a.loai_khach_hang,
  a.marketing_name AS staff_name,
  a.ma_nhan_vien AS id_staff,
  a.manager AS manager_name,
  a.ma_quan_ly AS ma_quan_ly,
  SUM(a.thanh_tien - a.chiet_khau) AS doanhThuLadi,
  SUM(a.doanh_so_moi) AS doanh_so_moi,
  SUM(a.doanh_so_cu) AS doanh_so_cu
FROM {{ ref('t2_pushsale_order_lines_toa') }} a
WHERE a.trang_thai_don_hang NOT IN ('Chờ chốt đơn','Hệ thống CRM đã xóa','Đã xóa') and a.nguon_doanh_thu <> 'Sàn TMDT liên kết với pushsale'
GROUP BY 
  DATE(a.ngay_chot_don),
  a.channel,
  a.brand,
  a.loai_khach_hang,
  a.marketing_name,
  a.ma_nhan_vien,
  a.manager,
  a.ma_quan_ly

  union all

  
SELECT
  DATE(a.ngay_chot_don) AS date_insert,
  a.channel,
  'Max Eagle' AS company,
  a.brand,
  a.loai_khach_hang,
  a.marketing_name AS staff_name,
  a.ma_nhan_vien AS id_staff,
  a.manager AS manager_name,
  a.ma_quan_ly AS ma_quan_ly,
  SUM(a.thanh_tien - a.chiet_khau) AS doanhThuLadi,
  SUM(a.doanh_so_moi) AS doanh_so_moi,
  SUM(a.doanh_so_cu) AS doanh_so_cu
FROM {{ref("t2_sandbox_order_lines_toa")}} a
WHERE a.trang_thai_don_hang NOT IN ('Chờ chốt đơn','Hệ thống CRM đã xóa','Đã xóa') and a.nguon_doanh_thu <> 'Sàn TMDT liên kết với pushsale'
GROUP BY 
  DATE(a.ngay_chot_don),
  a.channel,
  a.brand,
  a.loai_khach_hang,
  a.marketing_name,
  a.ma_nhan_vien,
  a.manager,
  a.ma_quan_ly