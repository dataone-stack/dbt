with order_tot as (
    select
        order_id,
        brand,
        'Order' as don_hang_san_pham,
        '-' as model_sku
        ngay_dat_hang,
        ngay_hoan_thanh_thanh_toan,
        ten_nguoi_mua,
        phuong_thuc_thanh_toan,
        ten_don_vi_van_chuyen,
        ngay_ship,
        ly_do_huy_don,
        order_status,
        doanh_thu_don_hang,
        tong_tien_san_pham,
        so_tien_hoan_lai,
        phi_van_chuyen_nguoi_mua_tra,
        phi_van_chuyen_thuc_te,
        phi_van_chuyen_tro_gia_tu_shopee,
        tro_gia_tu_shopee,
        ma_giam_gia,
        phi_co_dinh,
        phi_dich_vu,
        phi_thanh_toan,
        phi_hoa_hong_tiep_thi_lien_ket,
        0 as sort_order
    from {{ ref('t2_shopee_order_tot') }}
),
order_lines as (
    select
        order_id,
        brand,
        'SKU' as don_hang_san_pham,
        model_sku,
        create_time as ngay_dat_hang,
        null as ngay_hoan_thanh_thanh_toan,
        ten_nguoi_mua,
        hinh_thuc_thanh_toan as phuong_thuc_thanh_toan,
        ten_don_vi_van_chuyen,
        ngay_ship,
        ly_do_huy_don,
        order_status,
        doanh_thu_don_hang_uoc_tinh as doanh_thu_don_hang,
        tong_tien_san_pham,
        so_tien_hoan_tra as so_tien_hoan_lai,
        phi_van_chuyen_nguoi_mua_tra,
        phi_van_chuyen_thuc_te,
        phi_van_chuyen_tro_gia_tu_shopee,
        tro_gia_tu_shopee,
        voucher_from_seller as ma_giam_gia,
        phi_co_dinh,
        phi_dich_vu,
        phi_thanh_toan,
        phi_hoa_hong_tiep_thi_lien_ket,
        1 as sort_order
    from {{ ref('t2_shopee_order_lines_toa') }}
)

select *
from (
    select * from order_tot
    union all
    select * from order_lines
)
order by order_id, brand, sort_order, model_sku
