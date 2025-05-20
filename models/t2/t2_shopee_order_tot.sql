select 
    'Order' as don_hang_san_pham,
    vi.order_id,
    ord.create_time as ngay_dat_hang,
    vi.create_time as ngay_hoan_thanh_thanh_toan,
    ord.hinh_thuc_thanh_toan as phuong_thuc_thanh_toan,
    ord.doanh_thu_don_hang_uoc_tinh as doanh_thu_don_hang,
    ord.tong_tien_san_pham as tong_tien_san_pham,
    ord.so_tien_hoan_tra * -1 as so_tien_hoan_lai,
    ord.phi_van_chuyen_nguoi_mua_tra * -1 as phi_van_chuyen_nguoi_mua_tra,
    ord.phi_van_chuyen_thuc_te * -1 as phi_van_chuyen_thuc_te,
    ord.phi_van_chuyen_tro_gia_tu_shopee as phi_van_chuyen_tro_gia_tu_shopee,
    ord.tro_gia_tu_shopee as tro_gia_tu_shopee,
    ord.discount_from_voucher_seller as ma_giam_gia,
    
from {{ref("t1_shopee_shop_wallet_total")}} as vi
left join {{ref("t2_shopee_order_toa")}} as ord 
on vi.order_id = ord.order_id

