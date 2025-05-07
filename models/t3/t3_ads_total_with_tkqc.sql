select 
ads.date_start,
tkqc.idtkqc,
tkqc.nametkqc,
tkqc.ma_nhan_vien,
tkqc.staff,
tkqc.manager, 
tkqc.brand,
tkqc.channel, 
ads.spend as chiPhiAds, 
doanhThuAds as doanhThuAds 
from {{ ref('t2_ads_total') }} as ads right JOIN {{ ref('t1_tkqc')}} as tkqc
ON cast(ads.account_id as string) = tkqc.idtkqc
