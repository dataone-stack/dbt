SELECT 
    brand, 
    brand_lv1,
    company,
    -- company_lv1,
    sku,
    manager,
    channel,
    san_pham,
    thanh_tien,
    promotion_type,
    ngay_tien_ve_vi,
    ma_don_code,
    cast (ma_don_so as string) as ma_don_so,
    
    trang_thai_don_hang,
    tien_khach_hang_thanh_toan,
    ngay_chot_don,
    gia_ban_daily_total,
    tien_chiet_khau_sp,
    phu_phi,
    doanh_thu_ke_toan,
    doanh_thu_ke_toan as doanh_thu_ke_toan_v2,
    doanh_so_cu,
    doanh_so_moi,
    tracking_no,
    'Pushsale' as source
FROM {{ref("t2_pushsale_order_lines_tot")}}
WHERE trang_thai_don_hang NOT IN ('Chờ chốt đơn','Hệ thống CRM đã xóa','Đã xóa') and nguon_doanh_thu <> 'Sàn TMDT liên kết' and channel NOT IN ('Shopee', 'Tiktok')

UNION ALL

SELECT 
    s.brand, 
    s.brand_lv1,
    s.company,
    -- s.company_lv1,
    s.sku,
    s.manager,
    s.channel,
    s.san_pham,
    s.thanh_tien,
    s.promotion_type,
    s.ngay_tien_ve_vi,
    s.ma_don_code,
    s.ma_don_so,
    
    s.trang_thai_don_hang,
    s.tien_khach_hang_thanh_toan,
    s.ngay_chot_don,
    s.gia_ban_daily_total,
    s.tien_chiet_khau_sp,
    s.phu_phi,
    s.doanh_thu_ke_toan,
    s.doanh_thu_ke_toan as doanh_thu_ke_toan_v2,
    s.doanh_so_cu,
    s.doanh_so_moi,
    s.tracking_no,
    'Sandbox' as source
FROM {{ref("t2_sandbox_order_lines_tot")}} s
LEFT JOIN {{ref("t2_pushsale_order_lines_tot")}} p
    ON s.ma_don_code = p.ma_don_code
WHERE p.ma_don_code IS NULL and s.trang_thai_don_hang NOT IN ('Chờ chốt đơn','Hệ thống CRM đã xóa','Đã xóa') and s.nguon_doanh_thu <> 'Sàn TMDT liên kết' and s.channel NOT IN ('Shopee', 'Tiktok')