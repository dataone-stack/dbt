SELECT 
    '-' as manager,
    '-' AS marketing_name,
    "Shop Facebook" as shop,
    brand,
    brand_lv1,
    status as status,
    company,

    phi_van_chuyen_thuc_te,
    phi_van_chuyen_tro_gia_tu_san,
    phi_thanh_toan,
    phi_hoa_hong_shop,
    phi_hoa_hong_tiep_thi_lien_ket,
    phi_hoa_hong_quang_cao_cua_hang,
    phi_dich_vu,
    phi_xtra,
    voucher_from_seller,
    phi_co_dinh,
    tien_khach_hang_thanh_toan,
    tien_sp_sau_tro_gia,
    phi_ship,
    giam_gia_seller,
    giam_gia_san,
    seller_tro_gia,
    san_tro_gia,
    tong_phi_san,
   
    CAST(ma_don_hang AS string) AS ma_don_hang,
    ngay_tao_don,
    sku_code,
    ten_san_pham,
    CAST(so_luong AS INT64) AS so_luong,
    
    status_dang_don,
    gia_san_pham_goc,
    gia_san_pham_goc_total,
    gia_ban_daily,
    gia_ban_daily_total,
    tien_chiet_khau_sp,
    doanh_thu_ke_toan,
    doanh_thu_ke_toan AS doanh_so,
    'Facebook' AS channel,
    
    customer_name AS ten_khach_hang,

FROM {{ref("t2_facebook_order_lines_toa")}}

union all

SELECT 
    tkqc.manager,
    '-' AS marketing_name,
    shop.shop,
    shop.brand,
    shop.brand_lv1,
    shop.status,
    shop.company,
    phi_van_chuyen_thuc_te,
    phi_van_chuyen_tro_gia_tu_san,
    phi_thanh_toan,
    phi_hoa_hong_shop,
    phi_hoa_hong_tiep_thi_lien_ket,
    phi_hoa_hong_quang_cao_cua_hang,
    phi_dich_vu,
    phi_xtra,
    voucher_from_seller,
    phi_co_dinh,
    tien_khach_hang_thanh_toan,
    tien_sp_sau_tro_gia,
    phi_ship,
    giam_gia_seller,
    giam_gia_san,
    seller_tro_gia,
    san_tro_gia,
    tong_phi_san,

    CAST(ma_don_hang AS string) AS ma_don_hang,
    ngay_tao_don,
    sku_code,
    ten_san_pham,
    CAST(so_luong AS INT64) AS so_luong,
    shop.status as status_dang_don,
    gia_san_pham_goc,
    gia_san_pham_goc_total,
    gia_ban_daily,
    gia_ban_daily_total,
    tien_chiet_khau_sp,
    doanh_thu_ke_toan,
    doanh_so,
    'Shopee' AS channel,
    ten_nguoi_mua AS ten_khach_hang
FROM {{ref("t2_shopee_order_lines_toa")}} shop
LEFT JOIN `dtm.t2_tkqc_total` AS tkqc
        ON TRIM(CAST(shop.shop AS STRING)) = TRIM(CAST(tkqc.idtkqc AS STRING))
     
        AND DATE(shop.ngay_tao_don) >= DATE(tkqc.start_date)
        AND (tkqc.end_date IS NULL OR DATE(shop.ngay_tao_don) <= DATE(tkqc.end_date))


UNION ALL

SELECT 
    tkqc.manager,
    '-' AS marketing_name,
    shop.shop,
    shop.brand,
    shop.brand_lv1,
    shop.status,
    shop.company,

    phi_van_chuyen_thuc_te,
    phi_van_chuyen_tro_gia_tu_san,
    phi_thanh_toan * -1,
    phi_hoa_hong_shop * -1,
    phi_hoa_hong_tiep_thi_lien_ket * -1,
    phi_hoa_hong_quang_cao_cua_hang,
    phi_dich_vu * -1,
    phi_xtra * -1,
    voucher_from_seller,
    phi_co_dinh * -1,
    tien_khach_hang_thanh_toan,
    tien_sp_sau_tro_gia,
    phi_ship,
    giam_gia_seller,
    giam_gia_san,
    seller_tro_gia,
    san_tro_gia,
    tong_phi_san * -1,
    CAST(ma_don_hang AS string) AS ma_don_hang,
    ngay_tao_don,
    sku_code,
    ten_san_pham,
    CAST(so_luong AS INT64) AS so_luong,
    shop.status as status_dang_don,

    gia_san_pham_goc,
    gia_san_pham_goc_total,
    gia_ban_daily,
    gia_ban_daily_total,
    tien_chiet_khau_sp,
    doanh_thu_ke_toan,
    doanh_so,
    'Tiktok' AS channel,
    Recipient AS ten_khach_hang
FROM {{ref("t2_tiktok_order_line_toa")}} shop
LEFT JOIN `dtm.t2_tkqc_total` AS tkqc
        ON TRIM(CAST(shop.shop AS STRING)) = TRIM(CAST(tkqc.idtkqc AS STRING))
     
        AND DATE(shop.ngay_tao_don) >= DATE(tkqc.start_date)
        AND (tkqc.end_date IS NULL OR DATE(shop.ngay_tao_don) <= DATE(tkqc.end_date))


UNION ALL   

SELECT 
    manager,
    marketing_name,
    "Shop Facebook" as shop,
    brand,
    brand_lv1,
    trang_thai_don_hang as status,
    company,

    phi_van_chuyen_thuc_te,
    phi_van_chuyen_tro_gia_tu_san,
    phi_thanh_toan,
    phi_hoa_hong_shop,
    phi_hoa_hong_tiep_thi_lien_ket,
    phi_hoa_hong_quang_cao_cua_hang,
    phi_dich_vu,
    phi_xtra,
    voucher_from_seller,
    phi_co_dinh,

    tien_khach_hang_thanh_toan,
    tien_sp_sau_tro_gia,
    gia_dich_vu_vc as phi_ship,
    giam_gia_san_pham as giam_gia_seller,
    0 as giam_gia_san,
    seller_tro_gia,
    san_tro_gia,
    tong_phi_san,

    COALESCE(ma_don_code,CAST(ma_don_so AS STRING)) AS ma_don_hang,
    ngay_chot_don as ngay_tao_don,
    sku as sku_code,
    san_pham as ten_san_pham,
    CAST(so_luong AS INT64) AS so_luong,
    trang_thai_don_hang as status_dang_don,

    don_gia as gia_san_pham_goc,
    thanh_tien as gia_san_pham_goc_total,
    gia_ban_daily,
    gia_ban_daily_total,
    tien_chiet_khau_sp,
    doanh_thu_ke_toan,
    doanh_so,
    'Facebook' AS channel,
    ho_ten AS ten_khach_hang,
FROM {{ref("t2_mapping_sandbox_pushsale_toa")}}