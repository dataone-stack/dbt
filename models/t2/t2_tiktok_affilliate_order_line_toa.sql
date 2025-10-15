with aff_info as (
    select
        order_id,
        json_value(skus[OFFSET(0)], '$.content_id') as content_id,
        json_value(skus[OFFSET(0)], '$.content_type') as content_type,
        json_value(skus[OFFSET(0)], '$.creator_username') as creator_username,
        json_value(skus[OFFSET(0)], '$.campaign_id') as campaign_id,
        case
          when json_value(skus[OFFSET(0)], '$.shop_ads_commission_rate') is not null 
               and json_value(skus[OFFSET(0)], '$.commission_rate') is null
            then 'Ads'
          when json_value(skus[OFFSET(0)], '$.shop_ads_commission_rate') is null 
               and json_value(skus[OFFSET(0)], '$.commission_rate') is not null
            then 'Organic'
          else '-'
        end as type,
        brand,
        shop,
        company,
        safe_divide(cast(json_value(skus[OFFSET(0)], '$.shop_ads_commission_rate') as int64),10000) as shop_ads_commission_rate,
        safe_divide(cast(json_value(skus[OFFSET(0)], '$.commission_rate') as int64),10000) as commission_rate
    from {{ref("t1_tiktok_affiliate_order_total")}}
    where ARRAY_LENGTH(skus) > 0  -- <-- đảm bảo mảng có phần tử
)
, order_line as (
SELECT
  aff.order_id as id_don_hang,
  json_value(item,'$.product_id') as id_san_pham,
  bg.san_pham as ten_san_pham,
  CASE 
    WHEN json_value(item,'$.sku_name') IS NOT NULL 
    AND TRIM(json_value(item,'$.sku_name')) != '' 
    AND ARRAY_LENGTH(SPLIT(TRIM(json_value(item,'$.sku_name')), ',')) > 0 
    THEN SPLIT(TRIM(json_value(item,'$.sku_name')), ',')[SAFE_OFFSET(0)] 
    ELSE NULL 
  END AS color,
  CASE 
    WHEN json_value(item,'$.sku_name') IS NOT NULL 
    AND TRIM(json_value(item,'$.sku_name')) != '' 
    AND ARRAY_LENGTH(SPLIT(TRIM(json_value(item,'$.sku_name')), ',')) > 1 
    THEN SPLIT(TRIM(json_value(item,'$.sku_name')), ',')[SAFE_OFFSET(1)] 
    ELSE NULL 
  END AS size,
  json_value(item,'$.sku_id') as id_sku,
  json_value(item,'$.seller_sku') as sku_nguoi_ban,
  safe_cast(json_value(item,'$.sale_price') as int64) as gia,
  '-' as payment_amount,
  json_value(item,'$.currency') as don_vi_tien_te,
  1 as so_luong,
  ord.payment_method_name as phuong_thuc_thanh_toan,
  aff.status as trang_thai_don_hang,
  info.creator_username as ten_nguoi_dung,
  info.content_type as loai_noi_dung,
  info.content_id as id_noi_dung,
  0 as ty_le_khau_tru_thue_tncn,
  0 as thue_tncn_uoc_tinh,
  0 as thue_tncn_thuc_te,
  COALESCE(CAST(info.commission_rate * 100 AS INT64), 0) as ty_le_hoa_hong_tieu_chuan,
  safe_cast(json_value(item,'$.sale_price') as int64) as co_so_hoa_hong_uoc_tinh,
  COALESCE(CAST(safe_cast(json_value(item,'$.sale_price') as int64) * info.commission_rate AS INT64), 0) as thanh_toan_hoa_hong_tieu_chuan_uoc_tinh,
  0 as co_so_hoa_hong_thuc_te,
  0 as hoa_hong_thuc_te,
  COALESCE(CAST(info.shop_ads_commission_rate * 100 AS INT64), 0) as ty_le_hoa_hong_quang_cao_cua_hang,
  COALESCE(CAST(safe_cast(json_value(item,'$.sale_price') as int64) * info.shop_ads_commission_rate AS INT64), 0) as thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh,
  0 as thanh_toan_hoa_hong_quang_cao_cua_hang_thuc_te,
  0 as thuong_dong_chi_tra_cho_nha_sang_tao_uoc_tinh,
  0 as thuong_dong_chi_tra_cho_nha_sang_tao_thuc_te,
  0 as tra_hang_hoan_tien,
  0 as hoan_tien,
  DATETIME_ADD(aff.create_time, INTERVAL 7 HOUR) AS thoi_gian_da_tao,
  DATETIME_ADD(ord.paid_time, INTERVAL 7 HOUR) as thoi_gian_thanh_toan,
  '-' as thoi_gian_san_sang_van_chuyen,
  DATETIME_ADD(ord.delivery_time, INTERVAL 7 HOUR) as order_delivery_time,
  info.type as ads_org,
  aff.brand,
  aff.shop,
  aff.company,
bg.company_lv1,
  vid.title
FROM {{ref("t1_tiktok_affiliate_order_total")}} AS aff

LEFT JOIN {{ref("t1_tiktok_order_tot")}} AS ord ON aff.order_id = ord.order_id 
  AND aff.brand = ord.brand 
  AND aff.shop = ord.shop 
  AND aff.company = ord.company
LEFT JOIN aff_info as info ON aff.order_id = info.order_id 
  AND aff.brand = info.brand 
  AND aff.shop = info.shop 
  AND aff.company = info.company
LEFT JOIN {{ref("t1_tiktok_shop_video_info")}} vid on info.content_id = cast(vid.video_id as string)
CROSS JOIN UNNEST(COALESCE(ord.line_items, [])) AS item 

LEFT JOIN {{ref("t1_bang_gia_san_pham")}} bg on json_value(item,'$.seller_sku') = bg.ma_sku
)

select 
  *,
  thanh_toan_hoa_hong_tieu_chuan_uoc_tinh + thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh as hoa_hong_uoc_tinh,
  gia * so_luong as doanh_thu
  
from order_line