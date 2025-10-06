SELECT 
    brand, 
    -- brand_lv1,
    company,
    sku_code,
    ten_san_pham,
    gia_san_pham_goc_total,

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
    doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Facebook' AS channel,
    phi_van_chuyen_thuc_te,
    gia_von,
    promotion_type
FROM `crypto-arcade-453509-i8`.`dtm`.`pnl_t2_facebook`
where CAST(ngay_da_giao AS TIMESTAMP) is not null

union all

SELECT 
    brand, 
    -- brand_lv1,
    company,
    ma_san_pham as sku_code,
    ten_san_pham,
    gia_san_pham_goc_total,

    cast(ngay_ship as date) as ngay_ship,

    ngay_tien_ve_vi as date_create, 
    order_id,
    status, 
    tong_tien_da_thanh_toan as total_amount, 
    ngay_dat_hang as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_shopee as tien_chiet_khau_sp_tot,
    -- gia_san_pham_goc_total,
    phu_phi *-1 as phu_phi,
    doanh_thu_ke_toan as doanh_thu_ke_toan,
    doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Shopee' AS channel,
    phi_van_chuyen_thuc_te,
    gia_von,
    promotion_type
FROM `crypto-arcade-453509-i8`.`dtm`.`t2_shopee_order_lines_tot`
where status not in ("Đã hủy", "Đang giao")

UNION ALL

SELECT 
    brand, 
    -- brand_lv1,
    company,
    sku_code,
    ten_san_pham,
    gia_san_pham_goc_total,

    CAST(Shipped_Time as date) as ngay_ship,
    CAST(order_statement_time AS TIMESTAMP) as date_create, 
    CAST(ma_don_hang AS STRING) as order_id, 
    status, 
    total_settlement_amount as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    tong_phi_san *-1 as phu_phi,
    doanh_thu_ke_toan,
    doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Tiktok' AS channel,
    phi_van_chuyen_thuc_te * -1 as phi_van_chuyen_thuc_te,
    gia_von,
    promotion_type
FROM `crypto-arcade-453509-i8`.`dtm`.`t2_tiktok_order_line_tot`
-- where status not in ("Đã hủy", "Đang giao")



-- union all

-- SELECT 
--     brand, 
--     brand_lv1,
--     company,
--     -- sku as sku_code,
--     -- san_pham as ten_san_pham,
--     -- thanh_tien as gia_san_pham_goc_total,

--     cast(null as date) as ngay_ship,
--     ngay_tien_ve_vi as date_create, 
--     COALESCE(ma_don_code,CAST(ma_don_so AS STRING))  as order_id, 
--     trang_thai_don_hang as status, 
--     tien_khach_hang_thanh_toan as total_amount, 
--     ngay_chot_don as date_create_order, 
--     gia_ban_daily_total,
--     tien_chiet_khau_sp as tien_chiet_khau_sp_tot,

--     phu_phi,
--     doanh_thu_ke_toan,
--     doanh_thu_ke_toan_v2,
--     doanh_so_cu as doanh_so_cu,
--     doanh_so_moi as doanh_so_moi,
--     'Facebook' AS channel,
--     0 as phi_van_chuyen_thuc_te,
--     0 as gia_von
-- FROM `crypto-arcade-453509-i8`.`dtm`.`t2_mapping_sandbox_pushsale_tot`

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