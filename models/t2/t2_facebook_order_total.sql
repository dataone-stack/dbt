SELECT
    ord.brand,
    ord.id,
    ord.inserted_at,
    ord.updated_at,
    ord.status_name,
    ord.returned_reason_name,
    ord.total_price,
    ord.total_price_after_sub_discount,
    ord.total_quantity,
    JSON_EXTRACT_SCALAR(ord.page, '$.id') AS page_id,
    JSON_EXTRACT_SCALAR(ord.marketer, '$.name') AS marketer_name,
    JSON_EXTRACT_SCALAR(ord.customer, '$.name') AS nguoi_mua
FROM {{ref("t1_pancake_pos_order_total")}} AS ord
WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')