SELECT 
  DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) AS date_insert,
  'Facebook' AS channel,
  mar.company,
  od.brand,
  '' as brand_lv1,
  '' as loai_khach_hang, -- không có dữ liệu để NULL
  mar.marketing_name as staff_name,
  mar.ma_nhan_vien AS id_staff,
  mar.manager as manager_name,
  mar.ma_quan_ly AS ma_quan_ly,
  SUM(od.total_price_after_sub_discount) AS doanhThuMess   
FROM {{ref("t1_pancake_pos_order_total")}} AS od
LEFT JOIN {{ref("t1_marketer_facebook_total")}} AS mar
  ON JSON_VALUE(od.marketer, '$.name') = mar.marketer_name and DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) >= mar.start_date and DATE(DATE_ADD(od.inserted_at, INTERVAL 7 HOUR)) <= mar.end_date
WHERE od.marketer IS NOT NULL 
  AND mar.company = 'One5'
  AND (
        -- Điều kiện riêng cho brand LYB
        (od.brand = 'Chaching' AND od.order_sources_name = 'Facebook' and od.page is not null)
        -- Các brand khác
        OR (od.brand <> 'Chaching' AND od.order_sources_name IN ('Facebook'))
      )
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