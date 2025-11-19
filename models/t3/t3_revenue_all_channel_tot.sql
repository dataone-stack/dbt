with a as (SELECT 
    brand, 
    brand_lv1,
    company,
    -- company_lv1,
    "Shop Facebook" AS shop,
    sku_code AS sku,
    --manager,
    --marketing_name,
    ten_san_pham,
    -- gia_san_pham_goc_total,
    promotion_type,
    CAST(ngay_ship AS date) as ngay_ship,
    CAST(ngay_da_giao AS TIMESTAMP) as date_create, 
    ma_don_hang as order_id, 
    tracking_code,
    status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    0 as tax,
    doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
     0 as doanh_so_cu,
    0 as doanh_so_moi,
    case
    when order_sources_name  = 'Zalo'
    then 'Zalo'
    else 'Facebook' 
    end AS channel,
    0 as tong_phu_truoc_giam_gia,
    0 as giam_gia_cua_nguoi_ban,
    0 as tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban,

    0 as khoan_hoan_tien_giam_gia_cua_ban,
    0 as tong_doanh_thu,
    0 as tro_gia,
    0 as phi_giao_dich,
    0 as phi_dich_vu,
    0 as phi_co_dinh,
    0 as phi_hoa_hong_cua_tiktok_shop,
    0 as phi_van_chuyen_cua_nguoi_ban,
    0 as hoa_hong_lien_ket,
    0 as phi_dich_vu_voucher_xtra,
    0 as phi_xu_ly_don_hang,
    0 as thue_gtgt,
    0 as thue_tncn,
    0 as phi_dich_vu_sfr,
    0 as hoan_phi_sfr,
    '-' as check_hoan_phi_sfr,
    tong_phi_san

FROM {{ref("t2_facebook_order_lines_tot")}}
where CAST(ngay_da_giao AS TIMESTAMP) is not null

union all

SELECT 
    shop.brand, 
    shop.brand_lv1,
    shop.company,
    -- company_lv1,
    shop.shop,
    ma_san_pham AS sku,
    --tkqc.manager,
    --tkqc.staff AS marketing_name,
    ten_san_pham,

    promotion_type,
    cast(ngay_ship as date) as ngay_ship,

    ngay_tien_ve_vi as date_create, 
    order_id,
    '' as tracking_code,
    shop.status, 
    wallet_amount as total_amount, 
    ngay_dat_hang as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_shopee as tien_chiet_khau_sp_tot,
    -- gia_san_pham_goc_total,
    tong_chi_phi as phu_phi,
    tax,
    doanh_thu_ke_toan as doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Shopee' AS channel,

    gia_san_pham_goc_total as tong_phu_truoc_giam_gia,
    seller_tro_gia as giam_gia_cua_nguoi_ban,
    tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban,

    khoan_hoan_tien_giam_gia_cua_ban,
    gia_san_pham_goc_total + seller_tro_gia + tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban + khoan_hoan_tien_giam_gia_cua_ban as tong_doanh_thu,
    ma_giam_gia as tro_gia,
    phi_thanh_toan as phi_giao_dich,
    phi_dich_vu,
    phi_co_dinh,
    0 as phi_hoa_hong_cua_tiktok_shop,
    phi_van_chuyen_thuc_te + phi_van_chuyen_tro_gia_tu_san as phi_van_chuyen_cua_nguoi_ban,
    phi_hoa_hong_tiep_thi_lien_ket as hoa_hong_lien_ket,
    0 as phi_dich_vu_voucher_xtra,
    0 as phi_xu_ly_don_hang,
    thue_gtgt,
    thue_tncn,
    0 as phi_dich_vu_sfr,
    0 as hoan_phi_sfr,
    '-' as check_hoan_phi_sfr,
    tong_chi_phi as tong_phi_san
FROM {{ref("t2_shopee_order_lines_tot")}} as shop
-- LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t2_tkqc_total` AS tkqc
--         ON TRIM(CAST(shop.shop AS STRING)) = TRIM(CAST(tkqc.idtkqc AS STRING))
     
--         AND DATE(shop.ngay_dat_hang) >= DATE(tkqc.start_date)
--         AND (tkqc.end_date IS NULL OR DATE(shop.ngay_dat_hang) <= DATE(tkqc.end_date))
-- where status not in ("Đã hủy", "Đang giao")

UNION ALL

SELECT 
    shop.brand, 
    shop.brand_lv1,
    shop.company,
    -- company_lv1,
    shop.shop,
    sku_code AS sku,
    --tkqc.manager,
    --tkqc.staff AS marketing_name,
    ten_san_pham,
    -- gia_san_pham_goc_total,
    promotion_type,
    CAST(Shipped_Time as date) as ngay_ship,
    CAST(order_statement_time AS TIMESTAMP) as date_create, 
    CAST(adjustment_id AS STRING) as order_id, 
    '' as tracking_code,
    --Order_Status as status, 
    status,
    total_settlement_amount as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    tong_phi_san as phu_phi,
    tax as tax,
    doanh_thu_ke_toan as doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Tiktok' AS channel,

    gia_san_pham_goc_total as tong_phu_truoc_giam_gia,
    seller_tro_gia * 1 as giam_gia_cua_nguoi_ban,
    tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban,
    khoan_hoan_tien_giam_gia_cua_ban,
    gia_san_pham_goc_total + (seller_tro_gia * -1) + tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban + khoan_hoan_tien_giam_gia_cua_ban as  tong_doanh_thu,
    0 as tro_gia,
    phi_thanh_toan as phi_giao_dich,
    phi_dich_vu,
    0 as phi_co_dinh,

    phi_hoa_hong_shop as phi_hoa_hong_cua_tiktok_shop,
    seller_shipping_fee as phi_van_chuyen_cua_nguoi_ban,
    phi_hoa_hong_tiep_thi_lien_ket as hoa_hong_lien_ket,
    phi_xtra as phi_dich_vu_voucher_xtra,
    phi_xu_ly_don_hang,
    thue_gtgt,
    thue_tncn,
    phi_dich_vu_sfr,
    hoan_phi_sfr,
    check_hoan_phi_sfr,
    tong_phi_san

FROM {{ref("t2_tiktok_order_line_tot")}} as shop
-- LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t2_tkqc_total` AS tkqc
--         ON TRIM(CAST(shop.shop AS STRING)) = TRIM(CAST(tkqc.idtkqc AS STRING))
     
--         AND DATE(shop.ngay_tao_don) >= DATE(tkqc.start_date)
--         AND (tkqc.end_date IS NULL OR DATE(shop.ngay_tao_don) <= DATE(tkqc.end_date))
-- where status not in ("Đã hủy", "Đang giao")

union all

SELECT 
    brand, 
    brand_lv1,
    company,
    -- company_lv1,
    "Shop Facebook" AS shop,
    sku,
    --manager,
    --marketing_name,
    san_pham as ten_san_pham,
    -- thanh_tien as gia_san_pham_goc_total,
    promotion_type,
    cast(null as date) as ngay_ship,
    ngay_tien_ve_vi as date_create, 
    COALESCE(ma_don_code,CAST(ma_don_so AS STRING))  as order_id, 
    tracking_no as tracking_code,
    trang_thai_don_hang as status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_chot_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,

    phu_phi,
    0 as tax,
    doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
    doanh_so_cu as doanh_so_cu,
    doanh_so_moi as doanh_so_moi,
    'Facebook' AS channel,

    0 as tong_phu_truoc_giam_gia,
    0 as giam_gia_cua_nguoi_ban,
    0 as tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban,

    0 as khoan_hoan_tien_giam_gia_cua_ban,
    0 as tong_doanh_thu,
    0 as tro_gia,
    0 as phi_giao_dich,
    0 as phi_dich_vu,
    0 as phi_co_dinh,
    0 as phi_hoa_hong_cua_tiktok_shop,
    0 as phi_van_chuyen_cua_nguoi_ban,
    0 as hoa_hong_lien_ket,
    0 as phi_dich_vu_voucher_xtra,
    0 as phi_xu_ly_don_hang,
    0 as thue_gtgt,
    0 as thue_tncn,
    0 as phi_dich_vu_sfr,
    0 as hoan_phi_sfr,
    '-' as check_hoan_phi_sfr,
    0 as tong_phi_san
FROM {{ref("t2_mapping_sandbox_pushsale_tot")}}
)


select * from a