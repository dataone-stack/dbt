WITH total AS (
    SELECT 
        ord.id,
        ord.brand,
        SUM(ord.total_price) AS total_amount
    FROM {{ ref("t1_pancake_pos_order_total") }} AS ord
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')
    GROUP BY ord.id, ord.brand
),
fb_order_detail AS (
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
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) AS gia_san_phम्,
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64) AS dong_gia_khuyen_mai,
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) * 
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) - 
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64) AS tong_tien_san_pham,
        (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) * 
         SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) - 
         SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64)) / 
         tt.total_amount * SAFE_CAST(ord.total_discount AS FLOAT64) AS giam_gia_don_hang
    FROM {{ ref("t1_pancake_pos_order_total") }} AS ord,
    UNNEST(items) AS i
    LEFT JOIN total AS tt ON tt.id = ord.id AND tt.brand = ord.brand
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')
)
SELECT 
    fb.*,
    fb.tong_tien_san_pham - fb.giam_gia_don_hang AS tong_tien_san_pham_sau_khi_tru_cac_khuyen_mai
FROM fb_order_detail fb