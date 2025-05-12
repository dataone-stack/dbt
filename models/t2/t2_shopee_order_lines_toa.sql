WITH return_detail AS (
    SELECT 
        order_id, 
        i.item_id, 
        i.model_id,
        i.amount, 
        i.item_price, 
        i.amount * item_price AS so_tien_hoan_tra
    FROM {{ref("t1_shopee_shop_order_retrurn_total")}},
    UNNEST(item) AS i
),
sale_detail AS (
    SELECT
        order_id,
        order_status,
        DATETIME(TIMESTAMP_ADD(s.create_time, INTERVAL 7 HOUR)) AS create_time,
        i.model_sku,
        i.item_id,
        i.model_id,
        i.item_name,
        i.model_name,
        i.model_discounted_price,
        i.model_quantity_purchased,
        i.model_discounted_price * i.model_quantity_purchased AS gia_san_pham
    FROM {{ref("t1_shopee_shop_order_detail_total")}},
    UNNEST(item_list) AS i
),
sale_return_detail AS (
    SELECT 
        s.order_id,
        s.order_status,
        s.create_time,
        s.model_sku,
        s.item_name,
        s.model_name,
        s.model_discounted_price,
        s.model_quantity_purchased,
        s.gia_san_pham,
        r.amount,
        r.item_price,
        COALESCE(r.so_tien_hoan_tra, 0) AS so_tien_hoan_tra,
        s.gia_san_pham - COALESCE(r.so_tien_hoan_tra, 0) AS tong_tien_san_pham
    FROM sale_detail s
    LEFT JOIN return_detail r 
        ON s.order_id = r.order_id 
        AND s.item_id = r.item_id 
        AND s.model_id = r.model_id
),
total_amount AS (
    SELECT 
        order_id,
        SUM(tong_tien_san_pham) AS total_tong_tien_san_pham
    FROM sale_return_detail
    GROUP BY order_id
),
calculated_fees AS (
    SELECT
        sr.*,
        CASE
            WHEN ta.total_tong_tien_san_pham > 0
            THEN (sr.tong_tien_san_pham / ta.total_tong_tien_san_pham) * fee.actual_shipping_fee
            ELSE 0
        END AS phi_van_chuyen_thuc_te,
        CASE
            WHEN ta.total_tong_tien_san_pham > 0
            THEN (sr.tong_tien_san_pham / ta.total_tong_tien_san_pham) * fee.shopee_shipping_rebate
            ELSE 0
        END AS phi_van_chuyen_tro_gia_tu_shopee,
        CASE
            WHEN ta.total_tong_tien_san_pham > 0
            THEN (sr.tong_tien_san_pham / ta.total_tong_tien_san_pham) * fee.commission_fee
            ELSE 0
        END AS phi_co_dinh,
        CASE
            WHEN ta.total_tong_tien_san_pham > 0
            THEN (sr.tong_tien_san_pham / ta.total_tong_tien_san_pham) * fee.service_fee
            ELSE 0
        END AS phi_dich_vu,
        CASE
            WHEN ta.total_tong_tien_san_pham > 0
            THEN (sr.tong_tien_san_pham / ta.total_tong_tien_san_pham) * fee.seller_transaction_fee
            ELSE 0
        END AS phi_thanh_toan,
        i.discount_from_voucher_shopee AS shopee_voucher
    FROM sale_return_detail sr
    LEFT JOIN {{ref("t1_shopee_shop_fee_total")}} AS fee
        ON sr.order_id = fee.order_id
    LEFT JOIN UNNEST(fee.items) AS i
        ON sr.model_sku = i.model_sku
    LEFT JOIN total_amount ta
        ON sr.order_id = ta.order_id
)

SELECT
    *,
    CASE
        WHEN tong_tien_san_pham > 0
        THEN tong_tien_san_pham - phi_van_chuyen_thuc_te - phi_van_chuyen_tro_gia_tu_shopee
        else 0
    END AS doanh_thu_don_hang_sau_khi_tru_phi_van_chuyen,
    CASE
        WHEN tong_tien_san_pham > 0
        THEN tong_tien_san_pham - phi_van_chuyen_thuc_te - phi_van_chuyen_tro_gia_tu_shopee - phi_co_dinh - phi_dich_vu - phi_thanh_toan
        else 0
    END AS doanh_thu_thuc_cty_nhan_duoc,
    (gia_san_pham - shopee_voucher ) as so_tien_khach_phai_tra

FROM calculated_fees
