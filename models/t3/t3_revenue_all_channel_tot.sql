SELECT 
    brand, 
    CAST(ngay_da_giao AS TIMESTAMP) as date_create, 
    id as order_id, 
    trang_thai as status, 
    sku_code, 
    ten_san_pham as product_name, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    'Facebook' AS channel,
FROM {{ ref('t2_facebook_order_lines_tot') }}
where CAST(ngay_da_giao AS TIMESTAMP) is not null

UNION ALL

SELECT 
    brand, 
    ngay_hoan_thanh_thanh_toan as date_create, 
    order_id,
    "" as status, 
    ma_san_pham as sku_code, 
    ten_san_pham as product_name, 
    tong_tien_da_thanh_toan as total_amount, 
    ngay_dat_hang as date_create_order, 
    gia_ban_daily_total,
    'Shopee' AS channel
FROM {{ ref('t2_shopee_order_lines_tot') }}

UNION ALL

SELECT 
    brand, 
    CAST(order_statement_time AS TIMESTAMP) as date_create, 
    CAST(ma_don_hang AS STRING) as order_id, 
    Order_Status as status, 
    sku_code, 
    ten_san_pham as product_name, 
    total_settlement_amount as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    'Tiktok' AS channel
FROM {{ ref('t2_tiktok_order_line_tot') }}
