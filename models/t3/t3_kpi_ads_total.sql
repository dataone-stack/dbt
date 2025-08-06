with ads_total as (
  select 
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    sum(doanhThuads + doanhThuLadi) as DoanhThuAds,
    sum(chiPhiAds) as chiPhiAds,
    round(safe_divide(sum(chiPhiAds), sum(doanhThuads + doanhThuLadi)) , 4)  as cir
  from {{ref("t3_ads_total_with_tkqc")}}
  group by  
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager
)

select 
  a.*, 
  b.revenue_target,
  b.cir_target
from ads_total a 
left join {{ref("t1_kpi_ads_total")}} b 
  on a.brand = b.brand 
     and a.channel = b.channel 
     and a.ma_nhan_vien = b.ma_nhan_vien 
     and a.ma_quan_ly = b.manager_code
     and extract(month from a.date_start) = b.month
     and extract(year from a.date_start) = b.year