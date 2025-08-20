with total_price as (
  select
    id,
    brand,
    company,
    sum(total_price) as total_amount,
    json_value(customer, '$.name')  as customer_name,
    json_value(assigning_seller,'$.name') as nguoi_duoc_phan_cong,
    json_value(shipping_address,'$.district_name') as district,
    json_value(shipping_address,'$.province_name') as province,
    json_value(shipping_address,'$.commune_name') as commune
  from {{ref("t1_pancake_pos_order_total")}}
  group by id,brand,company,customer,assigning_seller,shipping_address
),

pancake_total as (

SELECT 
CASE 
    WHEN ARRAY_LENGTH(po.status_history) = 2 THEN 'Chưa xử lý'
    WHEN ARRAY_LENGTH(po.status_history) > 2 THEN 'Đã xử lý'
    ELSE 'Khác'
  END AS loai_don_no,
  po.*,

FROM {{ref("t1_pancake_pos_order_total")}} AS po
CROSS JOIN UNNEST(po.status_history) AS his
WHERE JSON_VALUE(his, '$.status') = '11' 

)

-- Tách từng item trong đơn hàng + mapping giá bán từ bảng giá sản phẩm
,order_line as (
  select
    ord.id,
    ord.loai_don_no,
    ord.brand,
    ord.order_sources_name,
    ord.company,
    ord.inserted_at,
    ord.updated_at,
    ord.status_name,
    ord.note_print,
    ord.activated_promotion_advances,
    -- Extract thông tin SKU, tên, màu, size từ JSON
    json_value(item, '$.variation_info.display_id')  as sku,
    json_value(item, '$.variation_info.name')  as ten_sp,
    json_value(item, '$.variation_info.fields[0].value') as color,
    json_value(item, '$.variation_info.fields[1].value') as size,
    safe_cast(json_value(item, '$.quantity') as int64) as so_luong,
    -- Giá gốc sản phẩm trước khuyến mãi
    COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64) + 
        safe_cast(json_value(item, '$.total_discount') as int64),
        safe_cast(json_value(item, '$.quantity') as int64)
    ), 0) as gia_goc,
    -- Khuyến mãi đồng giá seller
    safe_cast(json_value(item, '$.total_discount') as int64) as khuyen_mai_dong_gia,
    -- Phân bổ chiết khấu đơn hàng xuống từng dòng sản phẩm theo tỷ trọng giá trị
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
    -- Phân bổ phí vận chuyển xuống từng dòng sản phẩm
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
    -- Cước vận chuyển
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
    -- Trả trước
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
    -- Join thêm thông tin từ bảng total_price và bảng giá bán hàng ngày
    tt.customer_name,
    tt.nguoi_duoc_phan_cong,
    tt.district,
    tt.province,
    tt.commune,
    mapBangGia.gia_ban_daily,
    -- Flag thiếu hàng dựa trên added_to_cart_quantity
    case
      when json_value(item,'$.added_to_cart_quantity') = '0'
      then 'Thieu'
      else 'Du'
    end as con_thieu
  from pancake_total as ord,
  unnest (items) as item
  left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand
  left join {{ref("t1_bang_gia_san_pham")}} as mapBangGia on json_value(item, '$.variation_info.display_id') = mapBangGia.ma_sku
),
a as (
select
  id as ma_don_hang,
  loai_don_no,
  DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
  DATETIME_ADD(updated_at, INTERVAL 7 HOUR) as ngay_hoan_thanh,
  brand,
  company,
  customer_name,
  order_sources_name as channel,
  con_thieu,
  nguoi_duoc_phan_cong,
  district,
  province,
  commune,
  -- Tính số ngày nợ kể từ ngày tạo đơn hàng (cộng 7h GMT+7)
  DATE_DIFF(CURRENT_DATE, date(DATETIME_ADD(inserted_at, INTERVAL 7 HOUR)), DAY) as so_ngay_no,
  CASE
    WHEN DATE_DIFF(CURRENT_DATE, date(DATETIME_ADD(inserted_at, INTERVAL 7 HOUR)), DAY) BETWEEN 0 AND 2 THEN 'Dưới 3 ngày'
    WHEN DATE_DIFF(CURRENT_DATE, date(DATETIME_ADD(inserted_at, INTERVAL 7 HOUR)), DAY) BETWEEN 3 AND 5 THEN 'Từ 3 đến 5 ngày'
    WHEN DATE_DIFF(CURRENT_DATE, date(DATETIME_ADD(inserted_at, INTERVAL 7 HOUR)), DAY) BETWEEN 6 AND 7 THEN 'Từ 6 đến 7 ngày'
    WHEN DATE_DIFF(CURRENT_DATE, date(DATETIME_ADD(inserted_at, INTERVAL 7 HOUR)), DAY) BETWEEN 8 AND 14 THEN 'Từ 8 đến 14 ngày'
    ELSE 'Trên 14 ngày'
  END AS trang_thai_don_no,

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
  round((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang - phi_van_chuyen) as tien_khach_hang_thanh_toan,
  0 as tong_phi_san,
  round((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen) as tong_tien_sau_giam_gia,

  -- COD: nếu trả trước = số tiền khách thanh toán thì COD = 0; ngược lại = tổng tiền đơn hàng
    CASE
        WHEN (
        SELECT SUM(tra_truoc)
        FROM order_line ol2
        WHERE ol2.id = order_line.id
        ) = (
        SELECT SUM(ROUND((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang - phi_van_chuyen))
        FROM order_line ol3
        WHERE ol3.id = order_line.id
        ) THEN 0
        ELSE ROUND((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen)
    END AS cod,
  round(tra_truoc) as tra_truoc,
  cuoc_vc,
  round(phi_van_chuyen) as phi_ship,
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
  -- Trạng thái đơn hàng chi tiết (mapping thủ công theo business rule)
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
from order_line )

select * from a