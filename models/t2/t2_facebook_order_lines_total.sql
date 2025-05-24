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
        DATETIME_ADD(ord.inserted_at, INTERVAL 7 HOUR) AS inserted_at,
        ord.updated_at,
        ord.status_name,
        ord.returned_reason_name,
        JSON_EXTRACT_SCALAR(ord.page, '$.id') AS page_id,
        JSON_EXTRACT_SCALAR(ord.marketer, '$.name') AS marketer_name,
        JSON_EXTRACT_SCALAR(ord.customer, '$.name') AS ten_nguoi_mua,
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) AS quantity,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.display_id') AS sku,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.name') AS name,
        
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) + 
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64) AS gia_san_pham,

        (quantity * gia_san_pham) as tong_so_tien

    FROM {{ ref("t1_pancake_pos_order_total") }} AS ord
    CROSS JOIN UNNEST(COALESCE(ord.items, [])) AS i

    LEFT JOIN total AS tt ON tt.id = ord.id AND tt.brand = ord.brand
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')
)

SELECT 
    id,
    brand,
    inserted_at,
    updated_at,
    status_name,
    returned_reason_name,
    page_id,
    marketer_name,
    ten_nguoi_mua,
    quantity,
    sku,
    name,
    gia_san_pham,
    tong_so_tien
FROM fb_order_detail