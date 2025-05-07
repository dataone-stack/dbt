select ads.date_start,tkqc.idtkqc,tkqc.nametkqc,tkqc.ma_nhan_vien,tkqc.staff,tkqc.manager, tkqc.brand,tkqc.channel, sum(ads.chiPhi) as chiPhiAds from {{ ref('t2_ads_total') }} as ads
LEFT JOIN {{ ref('t1_tkqc')}} as tkqc
ON ads.account_id = tkqc.idtkqc