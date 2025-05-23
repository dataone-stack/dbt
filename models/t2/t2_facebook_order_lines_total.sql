select 
    ord.id,
    ord.brand,
    i.quantity
from {{ref("t1_pancake_pos_order_total")}} as ord,
UNNEST(item) AS i
where ord.order_sources_name in ('Facebook','Ladipage Facebook')

