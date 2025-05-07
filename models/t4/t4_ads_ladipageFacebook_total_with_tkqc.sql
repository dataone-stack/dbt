select ads.*,
ladi.doanhThuLadi as doanhThuLadi
from {{ref("t3_ads_total_with_tkqc")}} as ads
left join {{ref("t2_ladipage_facebook_total")}} as ladi
on ads.date_start = ladi.date_insert and ads.ma_nhan_vien = ladi.id_staff 
and ads.manager = ladi.manager and ads.brand = ladi.brand and ads.channel = ladi.channel