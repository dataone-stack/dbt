select 
    'Order' as don_hang_san_pham,
    vi.amount as test_doanh_thu,
    ord.brand,
    vi.order_id,
    ord.order_status,
    ord.create_time as ngay_dat_hang,
    DATETIME_ADD(vi.create_time, INTERVAL 7 HOUR) as ngay_hoan_thanh_thanh_toan,
    ord.hinh_thuc_thanh_toan as phuong_thuc_thanh_toan,
    ord.doanh_thu_don_hang_uoc_tinh as doanh_thu_don_hang,
    ord.tong_tien_san_pham as tong_tien_san_pham,
    ord.so_tien_hoan_tra * -1 as so_tien_hoan_lai,
    ord.phi_van_chuyen_nguoi_mua_tra * -1 as phi_van_chuyen_nguoi_mua_tra,
    ord.phi_van_chuyen_thuc_te * -1 as phi_van_chuyen_thuc_te,
    ord.phi_van_chuyen_tro_gia_tu_shopee as phi_van_chuyen_tro_gia_tu_shopee,
    ord.tro_gia_tu_shopee * -1 as tro_gia_tu_shopee,
    ord.discount_from_voucher_seller * -1 as ma_giam_gia,
    ord.phi_co_dinh * -1 as phi_co_dinh,
    ord.phi_dich_vu * -1 as phi_dich_vu,
    ord.phi_thanh_toan * -1 as phi_thanh_toan,
    ord.phi_hoa_hong_tiep_thi_lien_ket * -1 as phi_hoa_hong_tiep_thi_lien_ket
from {{ref("t1_shopee_shop_wallet_total")}} as vi
left join {{ref("t2_shopee_order_toa")}} as ord 
on vi.order_id = ord.order_id and vi.brand = ord.brand