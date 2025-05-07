select *,'Chaching' as brand from `chaching_pancake_pos_dwh.pancake_order`
union all
select * , 'LYB' as brand from `lyb_pancake_pos_dwh.pancake_order`
union all
select * , 'UME' as brand from `ume_pancake_pos_dwh.pancake_order`
union all
select * , 'LYB Cosmetics' as brand from `lybcosmetics_pancake_pos_dwh.pancake_order`