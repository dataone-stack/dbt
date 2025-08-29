SELECT 
    brand, 
    brand_lv1,
    company,
    CAST(ngay_ship AS date) as ngay_ship,
    CAST(ngay_da_giao AS TIMESTAMP) as date_create, 
    ma_don_hang as order_id, 
    status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
     0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Facebook' AS channel,
FROM {{ref("t2_facebook_order_lines_tot")}}
where CAST(ngay_da_giao AS TIMESTAMP) is not null

union all

SELECT 
    brand, 
    brand_lv1,
    company,
    cast(ngay_ship as date) as ngay_ship,

    ngay_tien_ve_vi as date_create, 
    order_id,
    status, 
    tong_tien_da_thanh_toan as total_amount, 
    ngay_dat_hang as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_shopee as tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_shopee as doanh_thu_ke_toan,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Shopee' AS channel
FROM {{ref("t2_shopee_order_tot")}}

UNION ALL

SELECT 
    brand, 
    brand_lv1,
    company,

    CAST(Shipped_Time as date) as ngay_ship,
    CAST(order_statement_time AS TIMESTAMP) as date_create, 
    CAST(ma_don_hang AS STRING) as order_id, 
    status, 
    total_settlement_amount as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    total_revenue as doanh_thu_ke_toan,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Tiktok' AS channel
FROM {{ref("t2_tiktok_order_tot")}}



union all

SELECT 
    brand, 
    brand_lv1,
    company,
    cast(null as date) as ngay_ship,
    ngay_tien_ve_vi as date_create, 
    COALESCE(ma_don_code,CAST(ma_don_so AS STRING))  as order_id, 
    trang_thai_don_hang as status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_data_ve as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
    doanh_so_cu as doanh_so_cu,
    doanh_so_moi as doanh_so_moi,
    'Facebook' AS channel
FROM {{ref("t2_mapping_sandbox_pushsale_tot")}}

-- union all

-- SELECT 
--     brand, 
--     company,
--     cast(null as date) as ngay_ship,
--     ngay_tien_ve_vi as date_create, 
--     COALESCE(ma_don_code,CAST(ma_don_so AS STRING))  as order_id, 
--     trang_thai_don_hang as status, 
--     tien_khach_hang_thanh_toan as total_amount, 
--     ngay_data_ve as date_create_order, 
--     gia_ban_daily_total,
--     tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
--     phu_phi,
--     doanh_thu_ke_toan,
--     'Facebook' AS channel
-- FROM `crypto-arcade-453509-i8`.`dtm`.`t2_sandbox_order_lines_tot`