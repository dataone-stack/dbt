WITH total  AS (
    SELECT 
        ord.id,
        ord.brand,
        SUM( SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) * 
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64)) AS total_amount
    FROM {{ ref("t1_pancake_pos_order_total") }} AS ord
    CROSS JOIN UNNEST(COALESCE(ord.items, [])) AS i
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook','Webcake')
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
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) AS quantity,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.display_id') AS sku,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.name') AS name,
        
        SAFE_DIVIDE(
        (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64)*
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) + 
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount')AS FLOAT64)),
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64)
        )
         AS gia_san_pham,

         SAFE_DIVIDE(
        (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64)*
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) + 
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount')AS FLOAT64)),
        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64)
        ) * SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64)
         as tong_so_tien,

        SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount')AS FLOAT64) AS khuyen_mai_dong_gia,

        SAFE_DIVIDE(
           SAFE_DIVIDE(
            (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64)*
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) + 
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount')AS FLOAT64)),
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64)
            ) * SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) - 
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount')AS FLOAT64),
            tt.total_amount
        ) * SAFE_CAST(ord.total_discount AS FLOAT64) AS giam_gia_don_hang,
        
        SAFE_DIVIDE(
            SAFE_DIVIDE(
            (SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS FLOAT64)*
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) + 
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount')AS FLOAT64)),
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64)
            ) * SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.quantity') AS FLOAT64) - 
            SAFE_CAST(JSON_EXTRACT_SCALAR(i, '$.total_discount')AS FLOAT64),
            tt.total_amount
        ) * SAFE_CAST(ord.shipping_fee AS FLOAT64) AS phi_van_chuyen


    FROM {{ ref("t1_pancake_pos_order_total") }} AS ord
    CROSS JOIN UNNEST(COALESCE(ord.items, [])) AS i

    LEFT JOIN total AS tt ON tt.id = ord.id AND tt.brand = ord.brand
    LEFT JOIN {{ref("t1_pancake_pos_product_total")}} as pr on pr.brand = ord.brand and pr.display_id = JSON_EXTRACT_SCALAR(i, '$.variation_info.display_id')
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook','Webcake')
)

SELECT 
    ord.*,
    (ord.tong_so_tien - ord.khuyen_mai_dong_gia - ord.giam_gia_don_hang + ord.phi_van_chuyen) as  tong_tien_can_thanh_toan,
    Case
        when pos.prepaid > 0
        then (ord.tong_so_tien - ord.khuyen_mai_dong_gia - ord.giam_gia_don_hang + ord.phi_van_chuyen)
        else 0
    end as tra_truoc,
     Case
        when pos.prepaid = 0
        then (ord.tong_so_tien - ord.khuyen_mai_dong_gia - ord.giam_gia_don_hang + ord.phi_van_chuyen)
        else 0
    end as cod,
FROM fb_order_detail as ord
left join {{ref("t1_pancake_pos_order_total")}} as pos
on ord.id = pos.id and ord.brand = pos.brand
WHERE pos.order_sources_name IN ('Facebook', 'Ladipage Facebook','Webcake')