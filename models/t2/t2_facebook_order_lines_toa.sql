with total_price as (
  select
    id,
    brand,
    company,
    sum(total_price) as total_amount,
    json_value(customer, '$.name')  as customer_name,
  from {{ref("t1_pancake_pos_order_total")}}
  group by id,brand,company,json_value(customer, '$.name')
)
,order_marketer_fix AS (
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
)
,
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
    --json_value(item, '$.variation_info.name')  as ten_sp,
    mapBangGia.san_pham as ten_sp,
    json_value(item, '$.variation_info.fields[0].value') as color,
    json_value(item, '$.variation_info.fields[1].value') as size,
    safe_cast(json_value(item, '$.quantity') as int64) as so_luong,
    -- COALESCE(
    --   SAFE_DIVIDE(
    --     safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
    --     safe_cast(json_value(item, '$.quantity') as int64) + 
    --     safe_cast(json_value(item, '$.total_discount') as int64),
    --     safe_cast(json_value(item, '$.quantity') as int64)
    --   ), 0) as gia_goc,
    COALESCE(safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64), 0) as gia_goc,
    COALESCE(safe_cast(json_value(item, '$.variation_info.retail_price') as int64), 0) as gia_sp_sau_giam_gia,

    safe_cast(json_value(item, '$.total_discount') as int64) as khuyen_mai_dong_gia,
    round(COALESCE(
      SAFE_DIVIDE(
        case
        when ord.brand in ('Chaching Beauty','An Cung') or json_value(item, '$.variation_info.retail_price_original') is null
        then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
        else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
        end
        --safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
        * safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.total_discount, 0) ) as giam_gia_don_hang,
    round(COALESCE(
      SAFE_DIVIDE(
        case
        when ord.brand in ('Chaching Beauty','An Cung') or json_value(item, '$.variation_info.retail_price_original') is null
        then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
        else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
        end
        * safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.shipping_fee, 0)) as phi_van_chuyen,
    round(COALESCE(
      SAFE_DIVIDE(
        case
        when ord.brand in ('Chaching Beauty','An Cung')or json_value(item, '$.variation_info.retail_price_original') is null
        then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
        else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
        end
        * safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.partner_fee, 0) ) as cuoc_vc,
    round(COALESCE(
      SAFE_DIVIDE(
        case
        when ord.brand in ('Chaching Beauty','An Cung')or json_value(item, '$.variation_info.retail_price_original') is null
        then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
        else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
        end
        * safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.prepaid, 0)) as tra_truoc,
    tt.customer_name,
    mapBangGia.gia_ban_daily,
    mapBangGia.brand_lv1,
    mapBangGia.company_lv1,
    mar.manager,
    mar.marketing_name
  from order_marketer_fix as ord,
  unnest (items) as item
  left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand
  left join {{ref("t1_bang_gia_san_pham")}} as mapBangGia on json_value(item, '$.variation_info.display_id') = mapBangGia.ma_sku
  left join {{ref("t1_marketer_facebook_total")}} mar on json_value(ord.marketer,'$.name') = mar.marketer_name and ord.brand = mar.brand
  where ord.order_sources_name not in ('Tiktok', 'Shopee') and ord.status_name not in ('removed')
)

, a as (
    select
  id as ma_don_hang,
  DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
  brand,
  brand_lv1,
  company,
  company_lv1,
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
  case
  when brand in ('Chaching Beauty','An Cung')or gia_goc = 0
  then (gia_sp_sau_giam_gia * so_luong) - khuyen_mai_dong_gia
  else  (gia_goc * so_luong) - khuyen_mai_dong_gia
  end as tien_sp_sau_tro_gia,

  case
  when brand in ('Chaching Beauty','An Cung')or gia_goc = 0
  then (gia_sp_sau_giam_gia * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen 
  else (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen 
  end as tien_khach_hang_thanh_toan,

  0 as tong_phi_san,

  case
  when brand in ('Chaching Beauty','An Cung')or gia_goc = 0
  then (gia_sp_sau_giam_gia * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen 
  else (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen 
  end as tong_tien_sau_giam_gia,
  
  case
  when tra_truoc > 0
  then 0
  when brand in ('Chaching Beauty','An Cung')or gia_goc = 0
  then (gia_sp_sau_giam_gia * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen
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
    --WHEN LOWER(note_print) LIKE '%ds%' OR LOWER(note_print) LIKE '%đổi size%' OR LOWER(note_print) like "%thu hồi%" or status_name in ('returned', 'returning') THEN 'Đã hoàn'
    WHEN status_name in ('returned', 'returning') THEN 'Đã hoàn'
    WHEN status_name in ('shipped','shipped') THEN 'Đang giao'
    WHEN status_name = 'canceled' THEN 'Đã hủy'
    WHEN status_name = 'delivered' THEN 'Đã giao thành công'
    WHEN status_name in ('new', 'packing', 'submitted','waitting', 'packing','pending') THEN 'Đăng đơn'
    ELSE 'Khác'
  END AS status,
  case
    when brand in ('Chaching Beauty','An Cung')or gia_goc = 0
    then (gia_sp_sau_giam_gia * so_luong) 
    else (gia_goc * so_luong) 
  end AS gia_san_pham_goc_total,

  COALESCE(gia_ban_daily, 0) AS gia_ban_daily,
  COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) AS gia_ban_daily_total,

  case
  when brand in ('Chaching Beauty','An Cung')or gia_goc = 0
  then (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_sp_sau_giam_gia * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang) 
  else (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang)
  end AS tien_chiet_khau_sp,

  
  case
  when brand in ('Chaching Beauty','An Cung')or gia_goc = 0
  then ((gia_sp_sau_giam_gia * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen) 
  else ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen) 
  end AS doanh_thu_ke_toan,
  manager,
  marketing_name
from order_line

),
don_no as (
  select distinct ma_don_hang, brand, sku_code, loai_don_no
  from {{ref("t2_pancake_pos_don_no_total")}}
)

select 
a.*,
case
  when b.loai_don_no is not null and a.status = 'Đăng đơn'
  then 'Đăng đơn nợ hàng'
  when b.loai_don_no is null and a.status = 'Đăng đơn'
  then 'Đăng đơn không nợ hàng'
  else a.status
end as status_dang_don
from a
left join don_no as b on a.ma_don_hang = b.ma_don_hang and a.brand = b.brand and a.sku_code = b.sku_code

