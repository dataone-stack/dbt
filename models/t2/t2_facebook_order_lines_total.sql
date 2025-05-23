with total as (
    select 
        ord.id,
        ord.brand,
        sum(ord.total_price) as total_amount
    from {{ref("t1_pancake_pos_order_total")}} as ord
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')
    group by ord.id, ord.brand
),
fb_order_detail as (
    SELECT
        ord.id,
        ord.brand,
        ord.inserted_at,
        ord.updated_at,
        ord.status_name,
        ord.returned_reason_name,
        JSON_EXTRACT_SCALAR(ord.page, '$.id') AS page_id,
        JSON_EXTRACT_SCALAR(ord.marketer, '$.name') AS marketer_name,
        JSON_EXTRACT_SCALAR(ord.customer, '$.name') AS ten_nguoi_mua,
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) AS quantity,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.display_id') AS sku,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.name') AS name,
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) AS gia_san_pham,
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64) AS dong_gia_khuyen_mai,
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) * 
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) - 
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64) as tong_tien_san_pham,
        (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) * 
         SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) - 
         SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64)) / 
         tt.total_amount * ord.total_discount AS giam_gia_don_hang
    FROM {{ref("t1_pancake_pos_order_total")}} AS ord,
    UNNEST(items) AS i
    LEFT JOIN total as tt ON tt.id = ord.id AND tt.brand = ord.brand
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')
)
select 
    fb.*,
    fb.tong_tien_san_pham - fb.giam_gia_don_hang as tong_tien_san_pham_sau_khi_tru_cac_khuyen_mai
from fb_order_detail fb