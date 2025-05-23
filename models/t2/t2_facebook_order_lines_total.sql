SELECT
    ord.id,
    ord.brand,
    SAFE_CAST(
        CASE
            WHEN i.quantity IS NOT NULL AND TRIM(i.quantity) REGEXP '^[0-9]+$' THEN i.quantity
            ELSE NULL
        END AS INT64
    ) AS quantity
FROM {{ref("t1_pancake_pos_order_total")}} AS ord,
UNNEST(items) AS i
WHERE ord.order_sources_name IN ('Facebook', 'Ladipage Facebook')