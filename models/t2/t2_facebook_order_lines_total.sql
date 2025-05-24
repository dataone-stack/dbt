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
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64) AS dong_gia_khuyen_mai,
        -- Tính giá trị sản phẩm sau chiết khấu cho từng mục
        (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) * 
         SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) - 
         SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64)) AS item_total,
        -- Tính tỷ lệ đóng góp của mục này so với tổng đơn hàng
        CASE 
            WHEN tt.total_amount > 0 THEN
                SAFE_DIVIDE(
                    (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64) * 
                     SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS INT64) - 
                     SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount') AS FLOAT64)),
                    tt.total_amount
                )
            ELSE 0 
        END AS item_ratio,
        SAFE_CAST(ord.shipping_fee AS FLOAT64) AS shipping_fee,
        SAFE_CAST(ord.total_discount AS FLOAT64) AS total_discount,
        SAFE_CAST(ord.total_price_after_sub_discount AS FLOAT64) AS total_price_after_sub_discount
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
    dong_gia_khuyen_mai,
    (item_ratio * shipping_fee) AS phi_van_chuyen,
    (item_ratio * total_discount) AS giam_gia_don_hang,
    (item_ratio * total_price_after_sub_discount) AS test_doanh_thu,
    (gia_san_pham * quantity - dong_gia_khuyen_mai + (item_ratio * shipping_fee)) AS tong_tien_san_pham,
    (gia_san_pham * quantity - dong_gia_khuyen_mai + (item_ratio * shipping_fee) - (item_ratio * total_discount)) AS tong_tien_san_pham_sau_khi_tru_cac_khuyen_mai
FROM fb_order_detail