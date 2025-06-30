SELECT 
    brand, 
    CAST(ngay_da_giao AS TIMESTAMP) as date_create, 
    ma_don_hang as order_id, 
    status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
    'Facebook' AS channel,
FROM {{ ref('t2_facebook_order_lines_tot') }}
where CAST(ngay_da_giao AS TIMESTAMP) is not null

UNION ALL

SELECT 
    brand, 
    ngay_tien_ve_vi as date_create, 
    order_id,
    "" as status, 
    tong_tien_da_thanh_toan as total_amount, 
    ngay_dat_hang as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
    'Shopee' AS channel
FROM {{ ref('t2_shopee_order_tot') }}

UNION ALL

SELECT 
    brand, 
    CAST(order_statement_time AS TIMESTAMP) as date_create, 
    CAST(ma_don_hang AS STRING) as order_id, 
    Order_Status as status, 
    total_settlement_amount as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
    'Tiktok' AS channel
FROM {{ ref('t2_tiktok_order_tot') }}
