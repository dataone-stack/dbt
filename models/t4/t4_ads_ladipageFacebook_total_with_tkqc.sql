with ladi_agg as (
  select 
    date_insert,
    id_staff,
    manager,
    brand,
    channel,
    sum(doanhThuLadi) as doanhThuLadi
  from {{ref("t2_ladipage_facebook_total")}}
  group by date_insert, id_staff, manager, brand, channel
)

select 
  ads.*,
  ladi_agg.doanhThuLadi
from {{ref("t3_ads_total_with_tkqc")}} as ads
left join ladi_agg
on ads.date_start = ladi_agg.date_insert 
and ads.ma_nhan_vien = ladi_agg.id_staff 
and ads.manager = ladi_agg.manager 
and ads.brand = ladi_agg.brand 
and ads.channel = ladi_agg.channel
