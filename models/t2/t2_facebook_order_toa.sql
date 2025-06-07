select
  ma_don_hang,
  ngay_tao_don,
  brand,
  status_name as trang_thai_don_hang,
  sum(so_luong) as so_luong,
  sum(gia_san_pham_goc) as gia_goc,
  sum(khuyen_mai_dong_gia) as khuyen_mai_dong_gia, 
  sum(giam_gia_don_hang) as giam_gia_don_hang,
  sum(tong_tien_sau_giam_gia) as tong_tien_sau_giam_gia,
  sum(cod) as cod,
  sum(tra_truoc) as tra_truoc,
  sum(cuoc_vc) as cuoc_vc,
  sum(phi_van_chuyen) as phi_van_chuyen
from {{ref("t2_facebook_order_lines_toa")}}
group by
  ma_don_hang,
  ngay_tao_don,
  brand,
  status_name
