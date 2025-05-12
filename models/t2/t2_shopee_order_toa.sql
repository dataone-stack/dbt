select ord.order_id,
ord.order_status,
ord.total_amount,
ord.estimated_shipping_fee,
ord.create_time,
ord.update_time,
ord.actual_shipping_fee,
ord.pickup_done_time,
ord.reverse_shipping_fee
from {{ref("t1_shopee_shop_order_total")}} as ord