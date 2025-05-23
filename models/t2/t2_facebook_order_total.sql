select
    ord.brand,
    ord.id,
    ord.inserted_at,
    ord.updated_at,
    ord.status_name,
    ord.returned_reason_name,
    ord.total_price,
    ord.total_price_after_sub_discount,
    ord.total_quantity,
    ord.json_value(page,"$.id") as page_id,
    ord.json_value(marketer,"$.name") as marketer_name,
    ord.json_value(customer,"$.name") as nguoi_mua

from {{ref("t1_pancake_pos_order_total")}} as ord
where ord.order_sources_name in ('Facebook','Ladipage Facebook')