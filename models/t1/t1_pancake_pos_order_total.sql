select updated_at,status_history,shipping_address, assigning_seller,prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,page,marketer,total_discount ,shop_id,'One5' as company, 'Chaching' as brand, customer, page_id from `chaching_pancake_pos_dwh.pancake_order`
union all
select updated_at,status_history,shipping_address, assigning_seller,prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,page,marketer,total_discount ,shop_id, 'One5' as company, 'LYB' as brand, customer, page_id from `lyb_pancake_pos_dwh.pancake_order`
union all
select updated_at,status_history,shipping_address, assigning_seller,prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,page,marketer,total_discount ,shop_id, 'One5' as company, 'UME' as brand, customer, page_id from `ume_pancake_pos_dwh.pancake_order`
union all
select updated_at,status_history,shipping_address, assigning_seller,prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price,order_sources_name ,page,marketer,total_discount ,shop_id, 'One5' as company, 'LYB Cosmetics', customer, page_id as brand from `lybcosmetics_pancake_pos_dwh.pancake_order`
union all
select updated_at,status_history,shipping_address, assigning_seller,prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price, order_sources_name ,page,marketer,total_discount ,shop_id,'One5' as company, 'An Cung' as brand, customer, page_id from `ancung_pancake_pos_dwh.pancake_order`
union all
select updated_at,status_history,shipping_address, assigning_seller,prepaid,partner_fee,shipping_fee,id,inserted_at,status_name,note_print,activated_promotion_advances,items,total_price_after_sub_discount,total_price, order_sources_name ,page,marketer,total_discount ,shop_id,'One5' as company, 'Chaching Beauty' as brand, customer, page_id from `chaching_beauty_pancake_pos_dwh.pancake_order`

