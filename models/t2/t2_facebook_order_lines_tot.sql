-- with total_price as (
--   select
--     id,
--     brand,
--     sum(total_price) as total_amount
--   from {{ref("t1_pancake_pos_order_total")}}
--   group by id,brand
-- ),

-- vietful_delivery_date as (
--   select 
--     brand,
--     partner_or_code,
--     shipped_date,
--     ref_code,
--     tracking_code,
--     (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
--      FROM UNNEST(status_trackings) AS status
--      WHERE JSON_VALUE(status, '$.statusCode') = '71'
--      LIMIT 1) AS ngay_da_giao
--   from {{ref("t1_vietful_xuatkho_total")}}
--   where sale_channel_code = 'PANCAKE'
-- ),
-- vietful_delivery_returned_date as (
--     SELECT 
--       brand,
--       partner_or_code,
--       ref_code,
--       shipped_date,
--       return_details,
--       sale_channel_code,
--       tracking_code,
--       (SELECT JSON_VALUE(status, '$.statusDate')
--       FROM UNNEST(status_trackings) AS status
--       WHERE JSON_VALUE(status, '$.statusCode') = '83'
--       LIMIT 1) AS ngay_da_giao
--   FROM {{ref("t1_vietful_xuatkho_total")}}
--   WHERE sale_channel_code = 'PANCAKE'
--     AND EXISTS (
--         SELECT 1
--         FROM UNNEST(status_trackings) AS status
--         WHERE JSON_VALUE(status, '$.statusCode') = '71'
--     )
--     AND EXISTS (
--         SELECT 1
--         FROM UNNEST(status_trackings) AS status
--         WHERE JSON_VALUE(status, '$.statusCode') = '83'
--     )

--   union all

--   SELECT 
--       brand,
--       partner_or_code,
--       ref_code,
--       shipped_date,
--       return_details,
--       sale_channel_code,
--       tracking_code,
--       (SELECT JSON_VALUE(status, '$.statusDate')
--        FROM UNNEST(status_trackings) AS status
--        WHERE JSON_VALUE(status, '$.statusCode') = '81'
--        LIMIT 1) AS ngay_da_giao
--   FROM {{ref("t1_vietful_xuatkho_total")}}
--   WHERE sale_channel_code = 'PANCAKE'
--     AND EXISTS (
--         SELECT 1
--         FROM UNNEST(status_trackings) AS status
--         WHERE JSON_VALUE(status, '$.statusCode') = '71'
--     )
--     AND EXISTS (
--         SELECT 1
--         FROM UNNEST(status_trackings) AS status
--         WHERE JSON_VALUE(status, '$.statusCode') = '81'
--     )
--     AND NOT EXISTS (
--         SELECT 1
--         FROM UNNEST(status_trackings) AS status
--         WHERE JSON_VALUE(status, '$.statusCode') = '83'
--     )
-- ),

-- vietful_return_detail as(
--    SELECT 
--     brand,
--     partner_or_code,
--     ref_code,
--     shipped_date,
--     ngay_da_giao,
--     tracking_code,
--     JSON_VALUE(i, '$.partnerSKU') AS partner_sku
--   FROM vietful_delivery_returned_date,
--   UNNEST(return_details) AS i
--   WHERE sale_channel_code = 'PANCAKE'
-- ),
-- order_line as (
--   select
--     ord.id,
--     ord.marketer,
--     ord.brand,
--     ord.order_sources_name,
--     ord.company,
--     ord.inserted_at,
--     ord.status_name,
--     ord.note_print,
--     ord.activated_promotion_advances,
--     json_value(item, '$.variation_info.display_id')  as sku,
--     json_value(item, '$.variation_info.name')  as ten_sp,
--     json_value(item, '$.variation_info.fields[0].value') as color,
--     json_value(item, '$.variation_info.fields[1].value') as size,
--     safe_cast(json_value(item, '$.quantity') as int64) as so_luong,
--     COALESCE(
--      safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64) , 0) as gia_goc,
--     COALESCE(
--      safe_cast(json_value(item, '$.variation_info.retail_price') as int64) , 0) as gia_goc_sau_giam_gia_san_pham,
--     safe_cast(json_value(item, '$.total_discount') as int64) as khuyen_mai_dong_gia,

--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.total_discount), 0) as giam_gia_don_hang,
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.shipping_fee), 0) as phi_van_chuyen,
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.partner_fee), 0) as cuoc_vc,
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.prepaid), 0) as tra_truoc,
--     mapBangGia.gia_ban_daily,
--     mapBangGia.brand_lv1,
--     vietful.ngay_da_giao,
--     vietful.tracking_code,
--     vietful.shipped_date as ngay_ship,
--     cost_price.cost_price as gia_von,
--     COALESCE(
--       ROUND(
--       SAFE_DIVIDE(
--         case
--         when json_value(item, '$.variation_info.retail_price_original') is null
--         then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--         else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--         end
--         *
--         safe_cast(json_value(item, '$.quantity') as int64),
--         NULLIF(tt.total_amount, 0)
--       ) * s.ship_fee), 0) as ship_fee

  
--   from {{ref("t1_pancake_pos_order_total")}} as ord,
--   unnest (items) as item
--   left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand
--   left join {{ref("t1_bang_gia_san_pham")}} as mapBangGia on json_value(item, '$.variation_info.display_id') = mapBangGia.ma_sku
--   left join `google_sheet.bang_gia_von` as cost_price on json_value(item, '$.variation_info.display_id') = cost_price.product_sku
--   left join vietful_delivery_date as vietful on CONCAT(ord.shop_id, '_', ord.id) = vietful.partner_or_code 
--   left join {{ref("t1_ship_fee")}} s on vietful.tracking_code = s.ma_van_don
--   where ord.order_sources_name not in ('Tiktok', 'Shopee') and ord.status_name not in ('removed')

-- ),

-- order_line_returned as (
--   select
--     ord.id,
--     ord.brand,
--     mapBangGia.brand_lv1,
--     ord.marketer,
--     ord.order_sources_name,
--     ord.company,
--     ord.inserted_at,
--     ord.status_name,
--     ord.note_print,
--     ord.activated_promotion_advances,
--     json_value(item, '$.variation_info.display_id')  as sku,
--     json_value(item, '$.variation_info.name')  as ten_sp,
--     json_value(item, '$.variation_info.fields[0].value') as color,
--     json_value(item, '$.variation_info.fields[1].value') as size,
--     safe_cast(json_value(item, '$.quantity') as int64) as so_luong,
--     COALESCE(
--      safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64) , 0) as gia_goc,
--     COALESCE(
--      safe_cast(json_value(item, '$.variation_info.retail_price') as int64) , 0) as gia_goc_sau_giam_gia_san_pham,
--     safe_cast(json_value(item, '$.total_discount') as int64) as khuyen_mai_dong_gia,
    
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.total_discount), 0) as giam_gia_don_hang,
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.shipping_fee), 0) as phi_van_chuyen,
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.partner_fee), 0) as cuoc_vc,
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * ord.prepaid), 0) as tra_truoc,
--     mapBangGia.gia_ban_daily,
--     vietful_return.ngay_da_giao,
--     vietful_return.tracking_code,
--     vietful_return.shipped_date as ngay_ship,
--     cost_price.cost_price as gia_von,
--     COALESCE(
--       ROUND(
--         SAFE_DIVIDE(
--           case
--           when json_value(item, '$.variation_info.retail_price_original') is null
--           then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
--           else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
--           end
--           *
--           safe_cast(json_value(item, '$.quantity') as int64),
--           NULLIF(tt.total_amount, 0)
--         ) * s.ship_fee), 0) as ship_fee

--   from {{ref("t1_pancake_pos_order_total")}} as ord,
--   unnest (items) as item
--   left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand
--   left join {{ref("t1_bang_gia_san_pham")}} as mapBangGia on json_value(item, '$.variation_info.display_id') = mapBangGia.ma_sku
--   left join `google_sheet.bang_gia_von` as cost_price on json_value(item, '$.variation_info.display_id') = cost_price.product_sku
--   left join vietful_return_detail as vietful_return on CONCAT(ord.shop_id, '_', ord.id) = vietful_return.partner_or_code and json_value(item, '$.variation_info.display_id') = vietful_return.partner_sku
--   left join {{ref("t1_ship_fee")}} s on vietful_return.tracking_code = s.ma_van_don
--   where ord.order_sources_name not in ('Tiktok', 'Shopee') and ord.status_name not in ('removed') and vietful_return.partner_sku is not null
-- )


-- ,order_delivered as (
--     select
--     id as ma_don_hang,
--     marketer,                                                                                
--     order_sources_name,                                                                  
--     DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
--     DATETIME_ADD(ngay_ship, INTERVAL 7 HOUR) as ngay_ship,
--     brand,
--     brand_lv1,
--     company,
--     status_name,
--     activated_promotion_advances,
--     sku as sku_code,
--     ten_sp as ten_san_pham,
--     color,
--     size,
--     so_luong,
--     gia_goc as gia_san_pham_goc,
--     khuyen_mai_dong_gia as giam_gia_seller,
--     giam_gia_don_hang as giam_gia_san,
--     0 as seller_tro_gia,
--     0 as san_tro_gia,
--     case
--     when brand in ('An Cung','Chaching Beauty')
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - khuyen_mai_dong_gia
    
--     else (gia_goc_sau_giam_gia_san_pham * so_luong)
--     end as tien_sp_sau_tro_gia,

--     case
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen  - ship_fee
--     when status_name = 'returned'
--     then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen  - ship_fee

--     when brand in ('An Cung','Chaching Beauty')
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia - ship_fee
--     else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee

--     end as tong_tien_da_thanh_toan,


--     case
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
--     when status_name = 'returned'
--     then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen 

--     when brand in ('An Cung','Chaching Beauty')
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia
--     else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen

--     end as tien_khach_hang_thanh_toan,

--     0 as tong_phi_san,

--     case
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
--     when status_name = 'returned'
--     then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen 

--     when brand in ('An Cung','Chaching Beauty')
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia
--     else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen 

    
--   end as tong_tien_sau_giam_gia,

--     case
--     when tra_truoc > 0
--     then 0
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
--     when status_name = 'returned'
--     then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen

--     when brand in ('An Cung','Chaching Beauty')
--     then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia
--     else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
--     end as cod,

--     tra_truoc,
--     cuoc_vc,
--     phi_van_chuyen as phi_ship,
--     ship_fee AS phi_van_chuyen_thuc_te,
--     0 AS phi_van_chuyen_tro_gia_tu_san,
--     0 AS phi_thanh_toan,
--     0 AS phi_hoa_hong_shop,
--     0 AS phi_hoa_hong_tiep_thi_lien_ket,
--     0 AS phi_hoa_hong_quang_cao_cua_hang,
--     0 AS phi_dich_vu,
--     0 as phi_xtra,
--     0 as voucher_from_seller,
--     0 as phi_co_dinh,
--     'Đã giao thành công' as status,
--     -------------------------------------------
--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         -- when gia_goc = 0
--         -- then (gia_goc_sau_giam_gia_san_pham * so_luong)
--         else (gia_goc_sau_giam_gia_san_pham * so_luong)
--     end as gia_san_pham_goc_total,

--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         else COALESCE(gia_ban_daily, 0)
--     end as gia_ban_daily,

--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         else COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)
--     end as gia_ban_daily_total,
--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--         then (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
--         when status_name ='returned'
--         then (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - giam_gia_don_hang)

--         when brand in ('An Cung','Chaching Beauty')
--         then (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia)
--         else (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
--     end as tien_chiet_khau_sp,

--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--         then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
--         when  status_name = 'returned'
--         then ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen)

--         when brand in ('An Cung','Chaching Beauty')
--         then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen-khuyen_mai_dong_gia)
--         else ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
--     end as doanh_thu_ke_toan,


--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--         then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
--         when status_name = 'returned'
--         then ((gia_goc * so_luong) - giam_gia_don_hang)

--         when brand in ('An Cung','Chaching Beauty')
--         then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia)
--         else ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
--     end as doanh_thu_ke_toan_v2,

--     ngay_da_giao,
--     tracking_code,
--     0 AS phu_phi,
--     gia_von* so_luong as gia_von,
--     case
--       when gia_goc_sau_giam_gia_san_pham = 0
--       then "Quà tặng"
--     end as promotion_type
--     from order_line
-- )


-- ,order_returned as (
--     select
--     id as ma_don_hang,
--     marketer,
--     order_sources_name,
--     DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
--     DATETIME_ADD(ngay_ship, INTERVAL 7 HOUR) as ngay_ship,
--     brand,
--     brand_lv1,
--     company,
--     status_name,
--     activated_promotion_advances,
--     sku as sku_code,
--     ten_sp as ten_san_pham,
--     color,
--     size,
--     so_luong,
--     gia_goc_sau_giam_gia_san_pham as gia_san_pham_goc,
--     khuyen_mai_dong_gia as giam_gia_seller,
--     giam_gia_don_hang as giam_gia_san,
--     0 as seller_tro_gia,
--     0 as san_tro_gia,
    
--     case
--     when brand in ('An Cung','Chaching Beauty')
--     then -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - khuyen_mai_dong_gia)
--     else -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong))
--     end as tien_sp_sau_tro_gia,

--     case
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee)
--     when status_name = 'returned'
--     then -1* ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee)

--     when brand in ('An Cung','Chaching Beauty')
--     then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen- khuyen_mai_dong_gia -ship_fee)
--     else -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee)
--     end as tong_tien_da_thanh_toan,

--     case
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
--     when status_name = 'returned'
--     then -1* ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen)

--     when brand in ('An Cung','Chaching Beauty')
--     then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen- khuyen_mai_dong_gia)
--     else -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
--     end as tien_khach_hang_thanh_toan,

--     0 as tong_phi_san,

--     case
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then -1* ( (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
--     when status_name = 'returned'
--     then -1* ( (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen )

--     when brand in ('An Cung','Chaching Beauty')
--     then -1* ( (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia )
--     else -1* ( (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen )
--     end as tong_tien_sau_giam_gia,

  
--     case
--     when tra_truoc > 0
--     then 0
--     when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--     then -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
--     when status_name = 'returned'
--     then -1 * ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen)

--     when brand in ('An Cung','Chaching Beauty')
--     then -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia)
--     else -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
--     end as cod,


--     tra_truoc,
--     cuoc_vc,
--     phi_van_chuyen as phi_ship,
--     ship_fee AS phi_van_chuyen_thuc_te,
--     0 AS phi_van_chuyen_tro_gia_tu_san,
--     0 AS phi_thanh_toan,
--     0 AS phi_hoa_hong_shop,
--     0 AS phi_hoa_hong_tiep_thi_lien_ket,
--     0 AS phi_hoa_hong_quang_cao_cua_hang,
--     0 AS phi_dich_vu,
--     0 as phi_xtra,
--     0 as voucher_from_seller,
--     0 as phi_co_dinh,
--     'Đã hoàn' as status,
--     --------------------------------------------------------

--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         -- when gia_goc = 0
--         -- then (gia_goc_sau_giam_gia_san_pham * so_luong)
--         else (gia_goc_sau_giam_gia_san_pham * so_luong)
--     end as gia_san_pham_goc_total,


--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         else COALESCE(gia_ban_daily, 0)
--     end as gia_ban_daily,
--     case
--         when gia_goc_sau_giam_gia_san_pham = 0
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         else COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) * -1
--     end as gia_ban_daily_total,

--     case
--         when gia_goc_sau_giam_gia_san_pham = 0 
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         when brand in ('An Cung','Chaching Beauty')  and status_name = 'returned'
--         then -1 * ((COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
--         when status_name = 'returned'
--         then -1 * ( (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - giam_gia_don_hang))

--         when brand in ('An Cung','Chaching Beauty') 
--         then -1 * ((COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia))
--         else -1 * ( (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
--     end as tien_chiet_khau_sp,

--     case
--         when gia_goc_sau_giam_gia_san_pham = 0 
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--         then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen))
--         when status_name = 'returned'
--         then -1 * (((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen))

--         when brand in ('An Cung','Chaching Beauty') 
--         then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen-khuyen_mai_dong_gia))
--         else -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen))
--     end as doanh_thu_ke_toan,

--     case
--         when gia_goc_sau_giam_gia_san_pham = 0 
--         then 0
--         -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
--         -- then 0
--         when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
--         then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
--         when status_name = 'returned'
--         then -1 * (((gia_goc * so_luong) - giam_gia_don_hang))
        
--         when brand in ('An Cung','Chaching Beauty') 
--         then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia))
--         else -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
--     end as doanh_thu_ke_toan_v2,

--     ngay_da_giao,
--     tracking_code,
--     0 AS phu_phi,
--     gia_von* so_luong as gia_von,
--     case
--       when gia_goc_sau_giam_gia_san_pham = 0
--       then "Quà tặng"
--     end as promotion_type

--     from order_line_returned
-- ),
                            
-- a as (
--   select * from order_delivered where ngay_da_giao is not null
--   union all
--   select * from order_returned where ngay_da_giao is not null
-- )

-- select * from a





with total_price as (
  select
    id,
    brand,
    sum(total_price) as total_amount,
    sum(total_price_after_sub_discount) as gia_tri_don_hang
  from {{ref("t1_pancake_pos_order_total")}}
  group by id,brand
),

vietful_delivery_date as (
  select 
    brand,
    partner_or_code,
    shipped_date,
    ref_code,
    tracking_code,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '71'
     LIMIT 1) AS ngay_da_giao
  from {{ref("t1_vietful_xuatkho_total")}} v
  where sale_channel_code = 'PANCAKE'
),
vietful_delivery_returned_date as (
    SELECT 
      brand,
      partner_or_code,
      ref_code,
      shipped_date,
      return_details,
      sale_channel_code,
      tracking_code,
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

  union all

  SELECT 
      brand,
      partner_or_code,
      ref_code,
      shipped_date,
      return_details,
      sale_channel_code,
      tracking_code,
      (SELECT JSON_VALUE(status, '$.statusDate')
       FROM UNNEST(status_trackings) AS status
       WHERE JSON_VALUE(status, '$.statusCode') = '81'
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
        WHERE JSON_VALUE(status, '$.statusCode') = '81'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM UNNEST(status_trackings) AS status
        WHERE JSON_VALUE(status, '$.statusCode') = '83'
    )
),

vietful_return_detail as(
   SELECT 
    brand,
    partner_or_code,
    ref_code,
    shipped_date,
    ngay_da_giao,
    tracking_code,
    JSON_VALUE(i, '$.partnerSKU') AS partner_sku
  FROM vietful_delivery_returned_date,
  UNNEST(return_details) AS i
  WHERE sale_channel_code = 'PANCAKE'
),
order_line as (
  select
    ord.id,
    ord.marketer,
    ord.brand,
    ord.order_sources_name,
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
     safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64) , 0) as gia_goc,
    COALESCE(
     safe_cast(json_value(item, '$.variation_info.retail_price') as int64) , 0) as gia_goc_sau_giam_gia_san_pham,
    safe_cast(json_value(item, '$.total_discount') as int64) as khuyen_mai_dong_gia,

    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.total_discount), 0) as giam_gia_don_hang,
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.shipping_fee), 0) as phi_van_chuyen,
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.partner_fee), 0) as cuoc_vc,
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.prepaid), 0) as tra_truoc,
    mapBangGia.gia_ban_daily,
    mapBangGia.brand_lv1,
    -- mapBangGia.company_lv1,
    vietful.ngay_da_giao,
    vietful.tracking_code,
    vietful.shipped_date as ngay_ship,
    cost_price.cost_price as gia_von,
    COALESCE(
      ROUND(
      SAFE_DIVIDE(
        case
        when json_value(item, '$.variation_info.retail_price_original') is null
        then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
        else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
        end
        *
        safe_cast(json_value(item, '$.quantity') as int64),
        NULLIF(tt.total_amount, 0)
      ) * s.ship_fee), 0) as ship_fee,
       CASE
    --WHEN LOWER(note_print) LIKE '%ds%' OR LOWER(note_print) LIKE '%đổi size%' OR LOWER(note_print) like "%thu hồi%" or status_name in ('returned', 'returning') THEN 'Đã hoàn'
    when tt.gia_tri_don_hang <= 60000 then 'Đổi size'
    WHEN ord.status_name in ('returned', 'returning') THEN 'Đã hoàn'
    WHEN ord.status_name in ('shipped','shipped') THEN 'Đang giao'
    WHEN ord.status_name = 'canceled' THEN 'Đã hủy'
    WHEN ord.status_name = 'delivered' THEN 'Đã giao thành công'
    WHEN ord.status_name in ('new', 'packing', 'submitted','waitting', 'packing','pending') THEN 'Đăng đơn'
    ELSE 'Khác'
  END AS status_don_hang,
   mar.manager,
   mar.marketing_name
      
    
  
  from {{ref("t1_pancake_pos_order_total")}} as ord,
  unnest (items) as item
  left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand
  left join {{ref("t1_bang_gia_san_pham")}} as mapBangGia on json_value(item, '$.variation_info.display_id') = mapBangGia.ma_sku
  left join `google_sheet.bang_gia_von` as cost_price on json_value(item, '$.variation_info.display_id') = cost_price.product_sku
  left join vietful_delivery_date as vietful on CONCAT(ord.shop_id, '_', ord.id) = vietful.partner_or_code 
  left join {{ref("t1_ship_fee")}} s on vietful.tracking_code = s.ma_van_don
  left join {{ref("t1_marketer_facebook_total")}} mar on json_value(ord.marketer,'$.name') = mar.marketer_name and ord.brand = mar.brand
  where ord.order_sources_name not in ('Tiktok', 'Shopee') and ord.status_name not in ('removed')

),

order_line_returned as (
  select
    ord.id,
    ord.brand,
    mapBangGia.brand_lv1,
    ord.marketer,
    ord.order_sources_name,
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
     safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64) , 0) as gia_goc,
    COALESCE(
     safe_cast(json_value(item, '$.variation_info.retail_price') as int64) , 0) as gia_goc_sau_giam_gia_san_pham,
    safe_cast(json_value(item, '$.total_discount') as int64) as khuyen_mai_dong_gia,
    
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.total_discount), 0) as giam_gia_don_hang,
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.shipping_fee), 0) as phi_van_chuyen,
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.partner_fee), 0) as cuoc_vc,
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * ord.prepaid), 0) as tra_truoc,
    mapBangGia.gia_ban_daily,
    -- mapBangGia.company_lv1,
    vietful_return.ngay_da_giao,
    vietful_return.tracking_code,
    vietful_return.shipped_date as ngay_ship,
    cost_price.cost_price as gia_von,
    COALESCE(
      ROUND(
        SAFE_DIVIDE(
          case
          when json_value(item, '$.variation_info.retail_price_original') is null
          then safe_cast(json_value(item, '$.variation_info.retail_price') as int64)
          else safe_cast(json_value(item, '$.variation_info.retail_price_original') as int64)
          end
          *
          safe_cast(json_value(item, '$.quantity') as int64),
          NULLIF(tt.total_amount, 0)
        ) * s.ship_fee), 0) as ship_fee,
           CASE
    --WHEN LOWER(note_print) LIKE '%ds%' OR LOWER(note_print) LIKE '%đổi size%' OR LOWER(note_print) like "%thu hồi%" or status_name in ('returned', 'returning') THEN 'Đã hoàn'
    when tt.gia_tri_don_hang <= 60000 then 'Đổi size'
    WHEN ord.status_name in ('returned', 'returning') THEN 'Đã hoàn'
    WHEN ord.status_name in ('shipped','shipped') THEN 'Đang giao'
    WHEN ord.status_name = 'canceled' THEN 'Đã hủy'
    WHEN ord.status_name = 'delivered' THEN 'Đã giao thành công'
    WHEN ord.status_name in ('new', 'packing', 'submitted','waitting', 'packing','pending') THEN 'Đăng đơn'
    ELSE 'Khác'
  END AS status_don_hang,
  mar.manager,
   mar.marketing_name

  from {{ref("t1_pancake_pos_order_total")}} as ord,
  unnest (items) as item
  left join total_price as tt on tt.id = ord.id and tt.brand = ord.brand
  left join {{ref("t1_bang_gia_san_pham")}} as mapBangGia on json_value(item, '$.variation_info.display_id') = mapBangGia.ma_sku
  left join `google_sheet.bang_gia_von` as cost_price on json_value(item, '$.variation_info.display_id') = cost_price.product_sku
  left join vietful_return_detail as vietful_return on CONCAT(ord.shop_id, '_', ord.id) = vietful_return.partner_or_code and json_value(item, '$.variation_info.display_id') = vietful_return.partner_sku
  left join {{ref("t1_ship_fee")}} s on vietful_return.tracking_code = s.ma_van_don
  left join {{ref("t1_marketer_facebook_total")}} mar on json_value(ord.marketer,'$.name') = mar.marketer_name and ord.brand = mar.brand
  where ord.order_sources_name in ('Facebook','Ladipage Facebook','Webcake','Website','') and ord.status_name not in ('removed') and vietful_return.partner_sku is not null
)


,order_delivered as (
    select
    id as ma_don_hang,
    marketer,                                                                                
    order_sources_name,                                                                  
    DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
    DATETIME_ADD(ngay_ship, INTERVAL 7 HOUR) as ngay_ship,
    brand,
    brand_lv1,
    company,
    -- company_lv1,
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
    when brand in ('An Cung','Chaching Beauty')
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - khuyen_mai_dong_gia
    
    else (gia_goc_sau_giam_gia_san_pham * so_luong)
    end as tien_sp_sau_tro_gia,

    case
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen  - ship_fee
    when status_name = 'returned'
    then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen  - ship_fee

    when brand in ('An Cung','Chaching Beauty')
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia - ship_fee
    else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee

    end as tong_tien_da_thanh_toan,


    case
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
    when status_name = 'returned'
    then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen 

    when brand in ('An Cung','Chaching Beauty')
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia
    else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen

    end as tien_khach_hang_thanh_toan,

    0 as tong_phi_san,

    case
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
    when status_name = 'returned'
    then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen 

    when brand in ('An Cung','Chaching Beauty')
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia
    else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen 

    
  end as tong_tien_sau_giam_gia,

    case
    when tra_truoc > 0
    then 0
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
    when status_name = 'returned'
    then (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen

    when brand in ('An Cung','Chaching Beauty')
    then (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia
    else (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen
    end as cod,

    tra_truoc,
    cuoc_vc,
    phi_van_chuyen as phi_ship,
    ship_fee AS phi_van_chuyen_thuc_te,
    0 AS phi_van_chuyen_tro_gia_tu_san,
    0 AS phi_thanh_toan,
    0 AS phi_hoa_hong_shop,
    0 AS phi_hoa_hong_tiep_thi_lien_ket,
    0 AS phi_hoa_hong_quang_cao_cua_hang,
    0 AS phi_dich_vu,
    0 as phi_xtra,
    0 as voucher_from_seller,
    0 as phi_co_dinh,
   status_don_hang,
    'Đã giao thành công' as status,
    -------------------------------------------
    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        -- when gia_goc = 0
        -- then (gia_goc_sau_giam_gia_san_pham * so_luong)
        else (gia_goc_sau_giam_gia_san_pham * so_luong)
    end as gia_san_pham_goc_total,

    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        else COALESCE(gia_ban_daily, 0)
    end as gia_ban_daily,

    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        else COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)
    end as gia_ban_daily_total,
    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
        then (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
        when status_name ='returned'
        then (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - giam_gia_don_hang)

        when brand in ('An Cung','Chaching Beauty')
        then (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia)
        else (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
    end as tien_chiet_khau_sp,

    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
        then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
        when  status_name = 'returned'
        then ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen)

        when brand in ('An Cung','Chaching Beauty')
        then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen-khuyen_mai_dong_gia)
        else ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
    end as doanh_thu_ke_toan,


    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
        then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
        when status_name = 'returned'
        then ((gia_goc * so_luong) - giam_gia_don_hang)

        when brand in ('An Cung','Chaching Beauty')
        then ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia)
        else ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang)
    end as doanh_thu_ke_toan_v2,

    ngay_da_giao,
    tracking_code,
    0 AS phu_phi,
    gia_von* so_luong as gia_von_total,
    gia_von,
    case
      when gia_goc_sau_giam_gia_san_pham = 0
      then "Quà tặng"
      ELSE "Hàng bán"
    end as promotion_type,
    manager,
   marketing_name
    from order_line
)


,order_returned as (
    select
    id as ma_don_hang,
    marketer,
    order_sources_name,
    DATETIME_ADD(inserted_at, INTERVAL 7 HOUR) as ngay_tao_don,
    DATETIME_ADD(ngay_ship, INTERVAL 7 HOUR) as ngay_ship,
    brand,
    brand_lv1,
    company,
    -- company_lv1,
    status_name,
    activated_promotion_advances,
    sku as sku_code,
    ten_sp as ten_san_pham,
    color,
    size,
    so_luong,
    gia_goc_sau_giam_gia_san_pham as gia_san_pham_goc,
    khuyen_mai_dong_gia as giam_gia_seller,
    giam_gia_don_hang as giam_gia_san,
    0 as seller_tro_gia,
    0 as san_tro_gia,
    
    case
    when brand in ('An Cung','Chaching Beauty')
    then -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - khuyen_mai_dong_gia)
    else -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong))
    end as tien_sp_sau_tro_gia,

    case
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee)
    when status_name = 'returned'
    then -1* ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee)

    when brand in ('An Cung','Chaching Beauty')
    then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen- khuyen_mai_dong_gia -ship_fee)
    else -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - ship_fee)
    end as tong_tien_da_thanh_toan,

    case
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
    when status_name = 'returned'
    then -1* ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen)

    when brand in ('An Cung','Chaching Beauty')
    then -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen- khuyen_mai_dong_gia)
    else -1* ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
    end as tien_khach_hang_thanh_toan,

    0 as tong_phi_san,

    case
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then -1* ( (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
    when status_name = 'returned'
    then -1* ( (gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen )

    when brand in ('An Cung','Chaching Beauty')
    then -1* ( (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia )
    else -1* ( (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen )
    end as tong_tien_sau_giam_gia,

  
    case
    when tra_truoc > 0
    then 0
    when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
    then -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
    when status_name = 'returned'
    then -1 * ((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen)

    when brand in ('An Cung','Chaching Beauty')
    then -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen - khuyen_mai_dong_gia)
    else -1 * ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen)
    end as cod,


    tra_truoc,
    cuoc_vc,
    phi_van_chuyen as phi_ship,
    ship_fee AS phi_van_chuyen_thuc_te,
    0 AS phi_van_chuyen_tro_gia_tu_san,
    0 AS phi_thanh_toan,
    0 AS phi_hoa_hong_shop,
    0 AS phi_hoa_hong_tiep_thi_lien_ket,
    0 AS phi_hoa_hong_quang_cao_cua_hang,
    0 AS phi_dich_vu,
    0 as phi_xtra,
    0 as voucher_from_seller,
    0 as phi_co_dinh,
    status_don_hang,
    'Đã hoàn' as status,
    --------------------------------------------------------

    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        -- when gia_goc = 0
        -- then (gia_goc_sau_giam_gia_san_pham * so_luong)
        else (gia_goc_sau_giam_gia_san_pham * so_luong)
    end as gia_san_pham_goc_total,


    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        else COALESCE(gia_ban_daily, 0)
    end as gia_ban_daily,
    case
        when gia_goc_sau_giam_gia_san_pham = 0
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        else COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) * -1
    end as gia_ban_daily_total,

    case
        when gia_goc_sau_giam_gia_san_pham = 0 
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        when brand in ('An Cung','Chaching Beauty')  and status_name = 'returned'
        then -1 * ((COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
        when status_name = 'returned'
        then -1 * ( (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc * so_luong) - giam_gia_don_hang))

        when brand in ('An Cung','Chaching Beauty') 
        then -1 * ((COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia))
        else -1 * ( (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - ((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
    end as tien_chiet_khau_sp,

    case
        when gia_goc_sau_giam_gia_san_pham = 0 
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
        then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen))
        when status_name = 'returned'
        then -1 * (((gia_goc * so_luong) - giam_gia_don_hang + phi_van_chuyen))

        when brand in ('An Cung','Chaching Beauty') 
        then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen-khuyen_mai_dong_gia))
        else -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen))
    end as doanh_thu_ke_toan,

    case
        when gia_goc_sau_giam_gia_san_pham = 0 
        then 0
        -- when (gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang + phi_van_chuyen < 50000
        -- then 0
        when brand in ('An Cung','Chaching Beauty') and status_name = 'returned'
        then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
        when status_name = 'returned'
        then -1 * (((gia_goc * so_luong) - giam_gia_don_hang))
        
        when brand in ('An Cung','Chaching Beauty') 
        then -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang - khuyen_mai_dong_gia))
        else -1 * (((gia_goc_sau_giam_gia_san_pham * so_luong) - giam_gia_don_hang))
    end as doanh_thu_ke_toan_v2,

    ngay_da_giao,
    tracking_code,
    0 AS phu_phi,
    gia_von * so_luong as gia_von_total,
    gia_von,
    case
      when gia_goc_sau_giam_gia_san_pham = 0
      then "Quà tặng"
    end as promotion_type,
    manager,
   marketing_name

    from order_line_returned
),
                            
a as (
  select * from order_delivered where ngay_da_giao is not null
  union all
  select * from order_returned where ngay_da_giao is not null
)

select * from a