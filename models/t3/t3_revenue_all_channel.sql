SELECT tien_khach_hang_thanh_toan,tien_sp_sau_tro_gia,phi_ship,giam_gia_seller,giam_gia_san,seller_tro_gia,san_tro_gia,tong_phi_san,brand, CAST(ma_don_hang AS string) AS ma_don_hang, ngay_tao_don, sku_code, ten_san_pham, CAST(so_luong AS INT64) AS so_luong, status, gia_san_pham_goc, gia_san_pham_goc_total, gia_ban_daily, gia_ban_daily_total, tien_chiet_khau_sp, doanh_thu_ke_toan, 'Facebook' AS channel,
FROM {{ ref('t2_facebook_order_lines_toa') }}
UNION ALL
SELECT tien_khach_hang_thanh_toan,tien_sp_sau_tro_gia,phi_ship,giam_gia_seller,giam_gia_san,seller_tro_gia,san_tro_gia,tong_phi_san,brand, CAST(ma_don_hang AS string) AS ma_don_hang, ngay_tao_don, sku_code, ten_san_pham, CAST(so_luong AS INT64) AS so_luong, status, gia_san_pham_goc, gia_san_pham_goc_total, gia_ban_daily, gia_ban_daily_total, tien_chiet_khau_sp, doanh_thu_ke_toan, 'Shopee' AS channel,
FROM {{ ref('t2_shopee_order_lines_toa') }}
UNION ALL
SELECT tien_khach_hang_thanh_toan,tien_sp_sau_tro_gia,phi_ship,giam_gia_seller,giam_gia_san,seller_tro_gia,san_tro_gia,tong_phi_san,brand, CAST(ma_don_hang AS string) AS ma_don_hang, ngay_tao_don, sku_code, ten_san_pham, CAST(so_luong AS INT64) AS so_luong, status, gia_san_pham_goc, gia_san_pham_goc_total, gia_ban_daily, gia_ban_daily_total, tien_chiet_khau_sp, doanh_thu_ke_toan, 'Tiktok' AS channel,
FROM {{ ref('t2_tiktok_order_line_toa') }}