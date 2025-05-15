with total_amount AS (
    SELECT 
        order_id,
        SUM(i.discounted_price) AS total_tong_tien_san_pham
    FROM {{ref("t1_shopee_shop_fee_total")}},   
    UNNEST(items) AS i

    GROUP BY order_id

)


select 
    toa.*,
    COALESCE(((toa.tong_tien_san_pham) / ta.total_tong_tien_san_pham) * wallet.amount , 0) as Tien_ve_vi_BQ,
    DATETIME_ADD(wallet.create_time, INTERVAL 7 HOUR) as ngay_tien_ve_vi
from {{ref("t1_shopee_shop_wallet_total")}} as wallet
LEFT JOIN {{ref("t2_shopee_order_lines_toa")}} as toa ON wallet.order_id = toa.order_id
LEFT JOIN total_amount as ta on wallet.order_id = ta.order_id
