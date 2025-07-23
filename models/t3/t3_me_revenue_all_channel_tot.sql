SELECT 
    brand, 
    company,
    CAST(ngay_da_giao AS TIMESTAMP) as date_create, 
    ma_don_hang as order_id, 
    status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
    'Facebook' AS channel,
FROM {{ ref('t2_facebook_order_lines_tot') }}
where CAST(ngay_da_giao AS TIMESTAMP) is not null and company = 'Max Eagle'
UNION ALL

SELECT 
    brand, 
    company,
    ngay_tien_ve_vi as date_create, 
    order_id,
    "" as status, 
    tong_tien_da_thanh_toan as total_amount, 
    ngay_dat_hang as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_shopee as tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_shopee as doanh_thu_ke_toan,
    'Shopee' AS channel
FROM {{ ref('t2_shopee_order_tot') }} where company = 'Max Eagle'

UNION ALL

SELECT 
    brand, 
    company,
    CAST(order_statement_time AS TIMESTAMP) as date_create, 
    CAST(ma_don_hang AS STRING) as order_id, 
    Order_Status as status, 
    total_settlement_amount as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    total_revenue as doanh_thu_ke_toan,
    'Tiktok' AS channel
FROM {{ ref('t2_tiktok_order_tot') }} where company = 'Max Eagle'

union all

SELECT 
    brand, 
    company,
    ngay_tien_ve_vi as date_create, 
    COALESCE(ma_don_code,CAST(ma_don_so AS STRING))  as order_id, 
    trang_thai_giao_hang as status, 
    tong_tien as total_amount, 
    ngay_data_ve as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
    'Pushsale' AS channel
FROM {{ref("t2_pushsale_order_lines_tot")}}

