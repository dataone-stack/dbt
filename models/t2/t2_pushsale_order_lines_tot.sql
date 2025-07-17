with orderline as (
SELECT 
  datetime_add(ord.create_time, INTERVAL 7 hour) AS ngay_data_ve, 
  ord.order_code AS ma_don,
  datetime_add(ord.order_confirm_date, INTERVAL 7 hour) AS ngay_chot_don,
  ord.warehouse_name AS kho,
  CONCAT(ord.shipping_carrier_name, ' (', ord.shipping_transport_name, ')') AS ptgh,
  ord.tracking_no AS ma_giao_van,
  '-' as ngay_cap_nhat_care_don,
  '-' as care_don,
  '-' as ghi_chu_ke_toan,
  datetime_add(ord.update_time, INTERVAL 7 hour) AS ngay_cap_nhat,
  '-' as trang_thai_giao_hang,
  datetime_add(ord.time_order_submit, INTERVAL 7 hour) as ngay_dang_don,
  ord.customer_name as ho_ten,
  ord.customer_phone as so_dien_thoai,
  '-' as ngay_muon_nhan_hang,
  concat(ord.delivery_address,', ',ord.delivery_ward_name,', ',ord.delivery_district_name,', ',ord.delivery_province_name) as dia_chi,
  ord.delivery_note as ghi_chu_giao_hang,
  dt.item_name as san_pham,
  dt.quantity as so_luong,
  dt.price as don_gia,
  dt.quantity * dt.price as thanh_tien,
  round((dt.quantity * dt.price) / NULLIF(ord.total_price, 0) * ord.total_discount) as chiet_khau,
  round((dt.quantity * dt.price) / NULLIF(ord.total_price, 0) * ord.total_cod) as gia_dich_vu_vc,
  round((dt.quantity * dt.price) / NULLIF(ord.total_price, 0) * case
    when ord.total_shipping_cost = 0
    then ord.total_cod
    else 0
  end, 2) as phi_vc_ho_tro_khach
FROM {{ref("t1_pushsale_order_line_total")}} dt
LEFT JOIN {{ref("t1_pushsale_order_total")}} ord 
ON dt.order_number = ord.order_number
)

select
  *,
  thanh_tien - COALESCE(chiet_khau, 0) + (COALESCE(gia_dich_vu_vc, 0) - COALESCE(phi_vc_ho_tro_khach, 0)) as tong_tien
from orderline