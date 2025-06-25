SELECT 
  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) AS date_insert,
  'Facebook' AS channel,
  od.brand,
  mar.staff,
  mar.ma_nhan_vien AS id_staff,
  mar.ma_quan_ly AS ma_quan_ly,
  SUM(od.total_price_after_sub_discount) AS doanhThuLadi
FROM {{ref('t1_pancake_pos_order_total')}} AS od
LEFT JOIN {{ref("t1_marketer_facebook_total")}} AS mar
  ON JSON_VALUE(od.marketer, '$.name') = mar.marketer_name
WHERE od.marketer IS NOT NULL 
  AND od.order_sources_name IN ('Facebook', 'Ladipage Facebook', 'Webcake')
  AND (od.brand != 'UME' OR (od.brand = 'UME' AND od.status_name NOT IN ('new', 'removed')))
GROUP BY 
  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)),
  mar.staff,
  mar.ma_nhan_vien,
  mar.ma_quan_ly,
  od.brand
