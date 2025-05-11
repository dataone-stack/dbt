SELECT 
  DATE(od.inserted_at) AS date_insert,
  'Facebook' AS channel,
  od.brand,
  mar.staff,
  sum(od.total_price_after_sub_discount) AS doanhThuLadi,
  mar.ma_nhan_vien AS id_staff,
  mar.manager AS manager,
FROM {{ref('t1_pancake_pos_order_total')}} AS od
left join {{ref("t1_marketer_facebook_total")}} as mar
on od.JSON_VALUE(od.marketer, "$.name") = mar.marketer_name
WHERE od.marketer IS NOT NULl
group by DATE(od.inserted_at),JSON_VALUE(od.marketer, "$.name"),id_staff,manager,od.brand