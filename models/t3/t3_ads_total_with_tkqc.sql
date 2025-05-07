select 
ads.date_start,
tkqc.idtkqc,
tkqc.nametkqc,
tkqc.ma_nhan_vien,
tkqc.staff,
tkqc.manager, 
tkqc.brand,
tkqc.channel, 
sum(ads.spend)as chiPhiAds, 
sum(doanhThuAds) as doanhThuAds 
from {{ ref('t2_ads_total') }} as ads right JOIN {{ ref('t1_tkqc')}} as tkqc
ON cast(ads.account_id as string) = tkqc.idtkqc
group by ads.date_start,tkqc.idtkqc,tkqc.nametkqc,tkqc.ma_nhan_vien,tkqc.staff,tkqc.manager, tkqc.brand,tkqc.channel