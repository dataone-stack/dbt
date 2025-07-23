with aff_info as(
    select
        order_id,
        json_value(skus[0],'$.content_type') as content_type,
        json_value(skus[0],'$.creator_username') as creator_username,
        case
        when json_value(skus[0],'$.shop_ads_commission_rate') is not null
        then 'Ads'
        else 'Organic'
        end as type,
        brand,
        shop,
        company
    from {{ref("t1_tiktok_affiliate_order_total")}}
)


SELECT
  aff.brand,
  date(DATETIME_ADD(aff.create_time, INTERVAL 7 HOUR)) AS date_create,
  aff.status,
  aff.order_id,
  json_value(item,'$.product_name') as product_name,
  json_value(item,'$.seller_sku') as seller_sku,
  SPLIT(json_value(item,'$.sku_name'), ',')[SAFE_OFFSET(0)] AS color,
  SPLIT(json_value(item,'$.sku_name'), ',')[SAFE_OFFSET(1)] AS size,
  info.content_type,
  info.creator_username,
  info.type as ads_org,
  case
    when info.type = 'Ads'
    then safe_cast(json_value(item,'$.sale_price') as int64) * 0.05
    else safe_cast(json_value(item,'$.sale_price') as int64) * 0.1
  end as hoa_hong,

  1 as quantity,

  safe_cast(json_value(item,'$.sale_price') as int64) as doanh_thu
FROM {{ref("t1_tiktok_affiliate_order_total")}} AS aff
LEFT JOIN {{ref("t1_tiktok_order_tot")}} AS ord ON aff.order_id = ord.order_id and aff.brand = ord.brand
LEFT JOIN aff_info as info ON aff.order_id = info.order_id and aff.brand = info.brand
CROSS JOIN UNNEST(ord.line_items) AS item