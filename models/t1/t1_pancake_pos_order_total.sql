select *,'Team A Tiến' as company, 'Chaching' as brand from `chaching_pancake_pos_dwh.pancake_order`
union all
select * , 'Team A Tiến' as company, 'LYB' as brand from `lyb_pancake_pos_dwh.pancake_order`
union all
select * , 'Team A Tiến' as company, 'UME' as brand from `ume_pancake_pos_dwh.pancake_order`
union all
select * , 'Team A Tiến' as company, 'LYB Cosmetics' as brand from `lybcosmetics_pancake_pos_dwh.pancake_order`
union all
select * , 'Team A Tiến' as company, 'An Cung' as brand from `ancung_pancake_pos_dwh.pancake_order`