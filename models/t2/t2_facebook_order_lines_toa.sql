with total_price as (
  select
    id,
    brand,
    sum(total_price) as total_amount
  from {{ref("t1_pancake_pos_order_total")}}
  group by id,brand
),
order_line as (
  select
    ord.id,
    ord.brand,
    ord.inserted_at,
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
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.total_discount, 0) as giam_gia_don_hang,
    COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.shipping_fee, 0) as phi_van_chuyen,
    COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.partner_fee, 0) as cuoc_vc,
    COALESCE(
      SAFE_DIVIDE(
        safe_cast(json_value(item, '$.variation_info.retail_price') as int64)*
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * ord.prepaid, 0) as tra_truoc
  from {{ref("t1_pancake_pos_order_total")}} as ord,
  unnest (items) as item
  left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand 
  and order_sources_name in ('Facebook','Ladipage Facebook','Webcake')
)
select
  id as ma_don_hang,
  DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
  brand,
  so_luong,
  gia_goc,
  khuyen_mai_dong_gia,
  giam_gia_don_hang,
  gia_goc * so_luong   - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen as tong_tien_sau_giam_gia,
  gia_goc * so_luong - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen - tra_truoc as cod,
  tra_truoc,
  cuoc_vc,
  phi_van_chuyen
from order_line

