SELECT 
    brand, 
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
    'Facebook' AS channel
FROM {{ ref('t2_pushsale_order_lines_tot') }}

UNION ALL

SELECT 
    s.brand, 
    s.company,
    cast(null as date) as ngay_ship,
    s.ngay_tien_ve_vi as date_create, 
    COALESCE(s.ma_don_code,CAST(s.ma_don_so AS STRING))  as order_id, 
    s.trang_thai_don_hang as status, 
    s.tien_khach_hang_thanh_toan as total_amount, 
    s.ngay_data_ve as date_create_order, 
    s.gia_ban_daily_total,
    s.tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    s.phu_phi,
    s.doanh_thu_ke_toan,
    'Facebook' AS channel
FROM {{ ref('t2_sandbox_order_lines_tot') }} s
LEFT JOIN {{ ref('t2_pushsale_order_lines_tot') }} p
    ON s.ma_don_code = p.ma_don_code
WHERE p.ma_don_code IS NULL
