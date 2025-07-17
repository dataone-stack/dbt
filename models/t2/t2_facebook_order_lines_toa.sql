with total_price as (
  select
    id,
    brand,
    company,
    sum(total_price) as total_amount,
    json_value(customer, '$.name')  as customer_name,
  from {{ref("t1_pancake_pos_order_total")}}
  group by id,brand,company,json_value(customer, '$.name')
),
order_line as (
  select
    ord.id,
    ord.brand,
    ord.company,
    ord.inserted_at,
    ord.status_name,
    ord.note_print,
    ord.activated_promotion_advances,
    json_value(item, '$.variation_info.display_id')  as sku,
    json_value(item, '$.variation_info.name')  as ten_sp,
    json_value(item, '$.variation_info.fields[0].value') as color,
    json_value(item, '$.variation_info.fields[1].value') as size,
    safe_cast(json_value(item, '$.quantity') as int64) as so_luong,
    COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64) + 
        safe_cast(json_value(item, '$.total_discount') as int64),
        safe_cast(json_value(item, '$.quantity') as int64)
      ), 0) as gia_goc,
    safe_cast(json_value(item, '$.total_discount') as int64) as khuyen_mai_dong_gia,
    COALESCE(
      SAFE_DIVIDE(
        COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64) + 
        safe_cast(json_value(item, '$.total_discount') as int64),
        safe_cast(json_value(item, '$.quantity') as int64)
      ), 0)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.total_discount, 0) as giam_gia_don_hang,
    COALESCE(
      SAFE_DIVIDE(
        COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64) + 
        safe_cast(json_value(item, '$.total_discount') as int64),
        safe_cast(json_value(item, '$.quantity') as int64)
      ), 0)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.shipping_fee, 0) as phi_van_chuyen,
    COALESCE(
      SAFE_DIVIDE(
        COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64) + 
        safe_cast(json_value(item, '$.total_discount') as int64),
        safe_cast(json_value(item, '$.quantity') as int64)
      ), 0)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.partner_fee, 0) as cuoc_vc,
    COALESCE(
      SAFE_DIVIDE(
        COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64) + 
        safe_cast(json_value(item, '$.total_discount') as int64),
        safe_cast(json_value(item, '$.quantity') as int64)
      ), 0)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.prepaid, 0) as tra_truoc,
    tt.customer_name,
    mapBangGia.gia_ban_daily
  from {{ref("t1_pancake_pos_order_total")}} as ord,
  unnest (items) as item
  left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand
  left join {{ref("t1_bang_gia_san_pham")}} as mapBangGia on json_value(item, '$.variation_info.display_id') = mapBangGia.ma_sku
  where ord.order_sources_name in ('Facebook','Ladipage Facebook','Webcake','') and ord.status_name not in ('removed')
)

select
  id as ma_don_hang,
  DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
  brand,
  company,
  customer_name,
  status_name,
  activated_promotion_advances,
  sku as sku_code,
  ten_sp as ten_san_pham,
  color,
  size,
  so_luong,
  gia_goc as gia_san_pham_goc,
  khuyen_mai_dong_gia as giam_gia_seller,
  giam_gia_don_hang as giam_gia_san,
  0 as seller_tro_gia,
  0 as san_tro_gia,
  (gia_goc * so_luong) - khuyen_mai_dong_gia as tien_sp_sau_tro_gia,
  (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang - phi_van_chuyen as tien_khach_hang_thanh_toan,
  0 as tong_phi_san,
  (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen as tong_tien_sau_giam_gia,
  case
  when tra_truoc > 0
  then 0
  else (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen
  end as cod,
  tra_truoc,
  cuoc_vc,
  phi_van_chuyen as phi_ship,
  0 AS phi_van_chuyen_thuc_te,
  0 AS phi_van_chuyen_tro_gia_tu_san,
  0 AS phi_thanh_toan,
  0 AS phi_hoa_hong_shop,
  0 AS phi_hoa_hong_tiep_thi_lien_ket,
  0 AS phi_hoa_hong_quang_cao_cua_hang,
  0 AS phi_dich_vu,
  0 as phi_xtra,
  0 as voucher_from_seller,
  0 as phi_co_dinh,
  CASE
    WHEN LOWER(note_print) LIKE '%ds%' OR LOWER(note_print) LIKE '%đổi size%' OR LOWER(note_print) like "%thu hồi%" or status_name in ('returned','pending', 'returning') THEN 'Đã hoàn'
    WHEN status_name in ('shipped','shipped') THEN 'Đang giao'
    WHEN status_name = 'canceled' THEN 'Đã hủy'
    WHEN status_name = 'delivered' THEN 'Đã giao thành công'
    WHEN status_name in ('new', 'packing', 'submitted','waitting', 'packing') THEN 'Đăng đơn'
    ELSE 'Khác'
  END AS status,
  (gia_goc * so_luong) AS gia_san_pham_goc_total,
  COALESCE(gia_ban_daily, 0) AS gia_ban_daily,
  COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) AS gia_ban_daily_total,
  (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang) AS tien_chiet_khau_sp,
  (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen)) AS doanh_thu_ke_toan
from order_line

