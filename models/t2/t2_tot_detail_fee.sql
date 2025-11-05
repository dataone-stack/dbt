with tiktok as (
  select 
  cast(ma_don_hang as string) as ma_don_hang ,company,brand,shop, ngay_tao_don,date(order_statement_time) as ngay_tien_ve ,
  sum(total_settlement_amount) as tong_so_tien_quyet_toan, 
  sum(gia_san_pham_goc_total) as tong_phu_truoc_giam_gia, 
  sum(seller_tro_gia * -1) as giam_gia_cua_nguoi_ban, 
  sum(tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban) as tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban,
  sum(khoan_hoan_tien_giam_gia_cua_ban) as khoan_hoan_tien_giam_gia_cua_ban,
  sum(gia_san_pham_goc_total + (seller_tro_gia * -1) + tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban + khoan_hoan_tien_giam_gia_cua_ban ) as tong_doanh_thu, 
  sum(phi_thanh_toan) as phi_giao_dich,
  sum(phi_dich_vu) as phi_dich_vu,
  0 as phi_co_dinh,
  sum(phi_hoa_hong_shop) as phi_hoa_hong_cua_tiktok_shop, 
  sum(seller_shipping_fee) as phi_van_chuyen_cua_nguoi_ban,
  sum(phi_hoa_hong_tiep_thi_lien_ket) as hoa_hong_lien_ket,
  sum(phi_xtra) as phi_dich_vu_voucher_xtra,
  sum(phi_xu_ly_don_hang) as phi_xu_ly_don_hang,
  sum(thue_gtgt) as thue_gtgt,
  sum(thue_tncn) as thue_tncn,
  sum(phi_dich_vu_sfr) as phi_dich_vu_sfr,
  0 as tro_gia,
  sum(tong_phi_san) as tong_phi_san
  
  from {{ref("t2_tiktok_order_line_tot")}} group by ma_don_hang, company,shop,ngay_tao_don, ngay_tien_ve,brand  
),
shopee_tot as (
  select order_id as ma_don_hang,company, brand,shop, ngay_dat_hang as ngay_tao_don, date(ngay_tien_ve_vi) as ngay_tien_ve , 
  sum(tong_tien_da_thanh_toan)  as tong_so_tien_quyet_toan
  from {{ref("t2_shopee_order_lines_tot")}}
  group by brand,shop,order_id, ngay_dat_hang, ngay_tien_ve_vi,company
),

shopee_toa as (
  select 
  ma_don_hang, brand, shop,
  sum(tong_tien_san_pham)  as tong_tien_san_pham,
  sum(so_tien_hoan_tra) as so_tien_hoan_tra,
  sum(gia_san_pham_goc * so_luong) as gia_san_pham_goc,
  sum(seller_tro_gia) as seller_tro_gia,

  sum(phi_van_chuyen_thuc_te) as phi_van_chuyen_thuc_te,
  sum(phi_van_chuyen_tro_gia_tu_san) as phi_van_chuyen_tro_gia_tu_san,
  sum(voucher_from_seller) as tro_gia,
  sum(phi_co_dinh) as phi_co_dinh,
  sum(phi_dich_vu) as phi_dich_vu,
  sum(phi_thanh_toan) as phi_thanh_toan,
  sum(phi_hoa_hong_tiep_thi_lien_ket) as phi_hoa_hong_tiep_thi_lien_ket,
  sum(thue_gtgt) as thue_gtgt,
  sum(thue_tncn) as thue_tncn,
  sum(tong_phi_san) as tong_phi_san,
  from {{ref("t2_shopee_order_lines_toa")}}
  group by ma_don_hang, brand,shop
),

shopee as (
  SELECT 
  a.*,
  b.gia_san_pham_goc as tong_phu_truoc_giam_gia,
  b.seller_tro_gia as giam_gia_cua_nguoi_ban,
  case
  when b.so_tien_hoan_tra > 0
  then  b.gia_san_pham_goc * -1
  else 0
  end as tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban,
  case
   when b.so_tien_hoan_tra > 0
    then  b.seller_tro_gia
    else 0
  end as khoan_hoan_tien_giam_gia_cua_ban,
  (b.tong_tien_san_pham - b.so_tien_hoan_tra) as tong_doanh_thu,
  b.phi_thanh_toan as phi_giao_dich,
  b.phi_dich_vu,
  b.phi_co_dinh,
  0 as phi_hoa_hong_cua_tiktok_shop,
  b.phi_van_chuyen_thuc_te +  b.phi_van_chuyen_tro_gia_tu_san as phi_van_chuyen_cua_nguoi_ban,
  b.phi_hoa_hong_tiep_thi_lien_ket as hoa_hong_lien_ket, 
  0 as phi_dich_vu_voucher_xtra,
  0 as phi_xu_ly_don_hang,
  b.thue_gtgt * -1 as thue_gtgt,
  b.thue_tncn * -1 as thue_tncn,
  0 as phi_dich_vu_sfr,
  b.tro_gia,
  b.tong_phi_san
  from shopee_tot a left join shopee_toa b on a.ma_don_hang = b.ma_don_hang and a.brand = b.brand and a.shop = b.shop

),
a as (
select *, 'Tiktok' as channel from tiktok 
union all
select *, 'Shopee' as channel from shopee)

select * from a 
