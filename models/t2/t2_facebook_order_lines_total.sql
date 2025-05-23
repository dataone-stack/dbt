with total as (
    select 
        ord.id,
        ord.brand,
        sum(ord.total_price) as total_amount,
    from {{ref("t1_pancake_pos_order_total")}} as ord
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')
    group by ord.id,ord.brand
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
        JSON_EXTRACT_SCALAR(i, '$.quantity') AS quantity,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.display_id') AS sku,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.name') AS name,
        JSON_EXTRACT_SCALAR(i, '$.variation_info.retail_price') AS gia_san_pham,
        JSON_EXTRACT_SCALAR(i, '$.total_discount') AS dong_gia_khuyen_mai,
        
    FROM `chaching_pancake_pos_dwh.pancake_order` AS ord,
    UNNEST(items) AS i
    left join total as tt on tt.id = ord.id and tt.brand = ord.brand
    WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')
)


    gia_san_pham * quantity - dong_gia_khuyen_mai as tong_tien_san_pham,

    (tong_tien_san_pham / ta.total_amount) * ord.total_discount AS giam_gia_don_hang,

    tong_tien_san_pham - giam_gia_don_hang as tong_tien_san_pham_sau_khi_tru_cac_khuyen_mai