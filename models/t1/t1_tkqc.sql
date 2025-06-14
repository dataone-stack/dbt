select cast(idtkqc as string) as idtkqc, nametkqc,ben_thue,phi_thue,cast(dau_the as int64) as dau_the,ma_nhan_vien,staff,ma_quan_ly,manager,brand,channel, 
status,start_date,end_date,'' as sku , 'Team A Tiến' as company from `google_sheet.tkqc` 
where idtkqc is not null
union all
select idtkqc, nametkqc,ben_thue,phi_thue,cast(dau_the as int64) as dau_the,ma_nhan_vien,staff,ma_quan_ly,manager,brand,channel,
'' as status, '2025-06-14' as start_date,'3000-12-31' as end_date, REGEXP_EXTRACT(brand, r'\((.*?)\)') as sku,'Max Eagle' as company
 from `google_sheet.tkqc_me`
 where idtkqc is not null