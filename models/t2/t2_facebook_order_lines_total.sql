select 
    ord.id,
    ord.brand,
    SAFE_CAST(i.quantity AS INT64) AS quantity
from {{ref("t1_pancake_pos_order_total")}} as ord,
UNNEST(items) AS i
where ord.order_sources_name in ('Facebook','Ladipage Facebook')

