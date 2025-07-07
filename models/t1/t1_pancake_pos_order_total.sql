select prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,marketer,total_discount ,shop_id,'One5' as company, 'Chaching' as brand from `chaching_pancake_pos_dwh.pancake_order`
union all
select prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,marketer,total_discount ,shop_id, 'One5' as company, 'LYB' as brand from `lyb_pancake_pos_dwh.pancake_order`
union all
select prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,marketer,total_discount ,shop_id, 'One5' as company, 'UME' as brand from `ume_pancake_pos_dwh.pancake_order`
union all
select prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,marketer,total_discount ,shop_id, 'One5' as company, 'LYB Cosmetics' as brand from `lybcosmetics_pancake_pos_dwh.pancake_order`
union all
select prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price, order_sources_name ,marketer,total_discount ,shop_id,'One5' as company, 'An Cung' as brand from `ancung_pancake_pos_dwh.pancake_order`
union all
select prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price, order_sources_name ,marketer,total_discount ,shop_id,'One5' as company, 'Chaching Beauty' as brand from `chaching_beauty_pancake_pos_dwh.pancake_order`

