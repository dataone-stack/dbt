SELECT 
  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) AS date_insert,
  'Facebook' AS channel,
  mar.company,
  od.brand,
  '' as loai_khach_hang,
  mar.marketing_name as staff_name,
  mar.ma_nhan_vien AS id_staff,
  mar.manager as manager_name,
  mar.ma_quan_ly AS ma_quan_ly,
  SUM(od.total_price_after_sub_discount) AS doanhThuLadi
FROM {{ref('t1_pancake_pos_order_total')}} AS od
LEFT JOIN {{ref("t1_marketer_facebook_total")}} AS mar
  ON JSON_VALUE(od.marketer, '$.name') = mar.marketer_name
WHERE od.marketer IS NOT NULL and mar.company = 'One5'
  AND od.order_sources_name IN ('Facebook', 'Ladipage Facebook', 'Webcake')
  AND (od.brand != 'UME' OR (od.brand = 'UME' AND od.status_name NOT IN ('new', 'removed')))
GROUP BY 
  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)),
  mar.ma_nhan_vien,
  mar.ma_quan_ly,
  od.brand,
  company,
  mar.manager,
  mar.ma_nhan_vien,
  mar.marketing_name,
  loai_khach_hang

union all

select
  date(a.ngay_chot_don) as date_insert,
  a.channel,
  'Max Eagle' as company,
  a.brand,
  a.loai_khach_hang,
  b.marketing_name as staff_name,
  b.ma_nhan_vien as id_staff,
  b.manager as manager_name,
  b.ma_quan_ly as ma_quan_ly,
  sum (a.thanh_tien - a.chiet_khau) as doanhThuLadi
from {{ref("t2_pushsale_order_lines_toa")}} a
left join {{ref("t1_marketer_facebook_total")}} b on a.marketing_user_name = b.marketer_name and a.company = b.company

where a.trang_thai_don_hang not in ('Chờ chốt đơn','Hệ thống CRM đã xóa', 'Đã xóa', 'Hủy vận đơn' )
group by date(a.ngay_chot_don),a.brand,b.ma_nhan_vien,b.ma_quan_ly,b.company, b.marketing_name, b.manager, a.channel, a.loai_khach_hang