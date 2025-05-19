SELECT 
   DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) AS date_insert,
  'Facebook' AS channel,
  od.brand,
  mar.staff,
  mar.ma_nhan_vien AS id_staff,
  mar.manager AS manager,
  sum(od.total_price_after_sub_discount) AS doanhThuLadi
FROM {{ref('t1_pancake_pos_order_total')}} AS od
left join {{ref("t1_marketer_facebook_total")}} as mar
on JSON_VALUE(od.marketer, '$.name') = mar.marketer_name
WHERE od.marketer IS NOT NULl
group by  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)),mar.staff,mar.ma_nhan_vien,mar.manager,od.brand