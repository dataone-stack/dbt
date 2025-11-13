with tkqc  as (
  select * from `dtm.t3_ads_total_with_tkqc` --where idtkqc in ('7415141571542368273','1360134442') and date(date_start) between '2025-11-09' and '2025-11-10'
),
shop as (
  select date(ngay_tao_don) as ngay_tao_don,shop_id, sum(tien_khach_hang_thanh_toan) as doanh_thu_san_test from `dtm.t3_revenue_all_channel` --where shop_id in ('VNLCQUWAWR','1360134442') and date(ngay_tao_don) between'2025-11-09' and '2025-11-10'
  group by date(ngay_tao_don),shop_id
)

select a.*, c.doanh_thu_san_test  from tkqc a 
left join `google_sheet.one_shop_id` b on a.idtkqc = cast(b.idtkqc as string)
left join shop c on b.shopid = c.shop_id and date(a.date_start) = date(c.ngay_tao_don) and a.loaiDoanhThu in ('TikTok GMVmax', 'Shopee Ads')



