WITH total_price AS (
  SELECT
    id,
    brand,
    SUM(total_price) AS total_amount
  FROM {{ref("t1_pancake_pos_order_total")}}
  GROUP BY id, brand
),
vietful_delivery_date AS (
  SELECT 
    brand,
    partner_or_code,
    ref_code,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '71'
     LIMIT 1) AS ngay_da_giao
  FROM {{ref("t1_vietful_xuatkho_total")}}
  WHERE sale_channel_code = 'PANCAKE'
),
vietful_delivery_returned_date AS (
  SELECT 
    brand,
    partner_or_code,
    ref_code,
    (SELECT JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '83'
     LIMIT 1) AS ngay_da_giao
  FROM {{ref("t1_vietful_xuatkho_total")}}
  WHERE sale_channel_code = 'PANCAKE'
    AND EXISTS (
      SELECT 1
      FROM UNNEST(status_trackings) AS status
      WHERE JSON_VALUE(status, '$.statusCode') = '71'
    )
    AND EXISTS (
      SELECT 1
      FROM UNNEST(status_trackings) AS status
      WHERE JSON_VALUE(status, '$.statusCode') = '83'
    )
),
order_line AS (
  SELECT
    ord.id,
    ord.brand,
    ord.company,
    ord.inserted_at,
    ord.status_name,
    ord.note_print,
    ord.activated_promotion_advances,
    JSON_VALUE(item, '$.variation_info.display_id') AS sku,
    JSON_VALUE(item, '$.variation_info.name') AS ten_sp,
    JSON_VALUE(item, '$.variation_info.fields[0].value') AS color,
    JSON_VALUE(item, '$.variation_info.fields[1].value') AS size,
    SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64) AS so_luong,
    COALESCE(SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price_original') AS INT64), 0) AS gia_goc,
    SAFE_CAST(JSON_VALUE(item, '$.total_discount') AS INT64) AS khuyen_mai_dong_gia,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.total_discount, 0) AS giam_gia_don_hang,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.shipping_fee, 0) AS phi_van_chuyen,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.partner_fee, 0) AS cuoc_vc,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.prepaid, 0) AS tra_truoc,
    mapBangGia.gia_ban_daily,
    vietful.ngay_da_giao
  FROM {{ref("t1_pancake_pos_order_total")}} AS ord,
    UNNEST(items) AS item
  LEFT JOIN total_price AS tt ON tt.id = ord.id AND tt.brand = ord.brand
  LEFT JOIN {{ref("t1_bang_gia_san_pham")}} AS mapBangGia ON JSON_VALUE(item, '$.variation_info.display_id') = mapBangGia.ma_sku
  LEFT JOIN vietful_delivery_date AS vietful ON CONCAT(ord.shop_id, '_', ord.id) = vietful.partner_or_code 
  WHERE ord.order_sources_name IN ('Facebook','Ladipage Facebook','Webcake','') AND ord.status_name NOT IN ('removed')
),
order_line_returned AS (
  SELECT
    ord.id,
    ord.brand,
    ord.company,
    ord.inserted_at,
    ord.status_name,
    ord.note_print,
    ord.activated_promotion_advances,
    JSON_VALUE(item, '$.variation_info.display_id') AS sku,
    JSON_VALUE(item, '$.variation_info.name') AS ten_sp,
    JSON_VALUE(item, '$.variation_info.fields[0].value') AS color,
    JSON_VALUE(item, '$.variation_info.fields[1].value') AS size,
    SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64) AS so_luong,
    COALESCE(SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price_original') AS INT64), 0) AS gia_goc,
    SAFE_CAST(JSON_VALUE(item, '$.total_discount') AS INT64) AS khuyen_mai_dong_gia,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.total_discount, 0) AS giam_gia_don_hang,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.shipping_fee, 0) AS phi_van_chuyen,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.partner_fee, 0) AS cuoc_vc,
    COALESCE(
      SAFE_DIVIDE(
        SAFE_CAST(JSON_VALUE(item, '$.variation_info.retail_price') AS INT64) *
        SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64),
        NULLIF(tt.total_amount, 0)
      ) * ord.prepaid, 0) AS tra_truoc,
    mapBangGia.gia_ban_daily,
    vietful.ngay_da_giao
  FROM {{ref("t1_pancake_pos_order_total")}} AS ord,
    UNNEST(items) AS item
  LEFT JOIN total_price AS tt ON tt.id = ord.id AND tt.brand = ord.brand
  LEFT JOIN {{ref("t1_bang_gia_san_pham")}} AS mapBangGia ON JSON_VALUE(item, '$.variation_info.display_id') = mapBangGia.ma_sku
  LEFT JOIN vietful_delivery_returned_date AS vietful ON CONCAT(ord.shop_id, '_', ord.id) = vietful.partner_or_code 
  WHERE ord.order_sources_name IN ('Facebook','Ladipage Facebook','Webcake','') AND ord.status_name NOT IN ('removed')
),
order_delivered AS (
  SELECT
    id AS ma_don_hang,
    DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) AS ngay_tao_don,
    brand,
    company,
    status_name,
    activated_promotion_advances,
    sku AS sku_code,
    ten_sp AS ten_san_pham,
    color,
    size,
    so_luong,
    gia_goc AS gia_san_pham_goc,
    khuyen_mai_dong_gia AS giam_gia_seller,
    giam_gia_don_hang,
    0 AS giam_gia_san,
    0 AS seller_tro_gia,
    0 AS san_tro_gia,
    (gia_goc * so_luong) - khuyen_mai_dong_gia AS tien_sp_sau_tro_gia,
    (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang - phi_van_chuyen AS tien_khach_hang_thanh_toan,
    0 AS tong_phi_san,
    (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen AS tong_tien_sau_giam_gia,
    CASE
      WHEN tra_truoc > 0 THEN 0
      ELSE (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen
    END AS cod,
    tra_truoc,
    cuoc_vc,
    phi_van_chuyen AS phi_ship,
    0 AS phi_van_chuyen_thuc_te,
    0 AS phi_van_chuyen_tro_gia_tu_san,
    0 AS phi_thanh_toan,
    0 AS phi_hoa_hong_shop,
    0 AS phi_hoa_hong_tiep_thi_lien_ket,
    0 AS phi_hoa_hong_quang_cao_cua_hang,
    0 AS phi_dich_vu,
    0 AS phi_xtra,
    0 AS voucher_from_seller,
    0 AS phi_co_dinh,
    'Đã giao hàng' AS status,
    CASE
      WHEN gia_goc = 0 THEN 0
      WHEN (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen < 50000 THEN 0
      ELSE (gia_goc * so_luong)
    END AS gia_san_pham_goc_total,
    COALESCE(gia_ban_daily, 0) AS gia_ban_daily,
  
    COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) as gia_ban_daily_total,
    (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang) AS tien_chiet_khau_sp,
    ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang) AS doanh_thu_ke_toan,
    ngay_da_giao,
    0 AS phu_phi
  FROM order_line
),
order_returned AS (
  select
    id as ma_don_hang,
    DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
    brand,
    company,
    status_name,
    activated_promotion_advances,
    sku as sku_code,
    ten_sp as ten_san_pham,
    color,
    size,
    so_luong,
    gia_goc as gia_san_pham_goc,
    khuyen_mai_dong_gia as giam_gia_seller,
    giam_gia_don_hang ,
    0 as giam_gia_san,
    0 as seller_tro_gia,
    0 as san_tro_gia,
    (gia_goc * so_luong) - khuyen_mai_dong_gia as tien_sp_sau_tro_gia,
    -1* ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang - phi_van_chuyen) as tien_khach_hang_thanh_toan,
    0 as tong_phi_san,
    (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen as tong_tien_sau_giam_gia,
    case
    when tra_truoc > 0
    then 0
    else -1 * ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen)
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
    'Hoàn tất trả hàng' as status,
    case
      when gia_goc = 0
      then 0
      when (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen < 50000
      then 0
      else (gia_goc * so_luong)
    end as gia_san_pham_goc_total,
    case
      when gia_goc = 0
      then 0
      when (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen < 50000
      then 0
      else COALESCE(gia_ban_daily, 0)
    end as gia_ban_daily,
    case
      when gia_goc = 0
      then 0
      when (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen < 50000
      then 0
      else COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) * -1
    end as gia_ban_daily_total,
    case
      when gia_goc = 0
      then 0
      when (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen < 50000
      then 0
      else (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang)
    end as tien_chiet_khau_sp,
    case
      when gia_goc = 0
      then 0
      when (gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang + phi_van_chuyen < 50000
      then 0
      else ((gia_goc * so_luong) - khuyen_mai_dong_gia - giam_gia_don_hang) * -1
    end as doanh_thu_ke_toan,
    ngay_da_giao,
    0 AS phu_phi
  from order_line_returned
),
a AS (
  SELECT * FROM order_delivered
  UNION ALL
  SELECT * FROM order_returned WHERE ngay_da_giao IS NOT NULL
)
SELECT * FROM a
