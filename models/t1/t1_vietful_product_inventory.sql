select warehouse_code, partner_sku, sku, unit_code, condition_type_code, date_record, physical_qty, available_qty, pending_in_qty, pending_out_qty, 	
freeze_qty, 'Chaching' as brand  from `chaching_vietful_dwh.vietful_product_inventory_lyb`
UNION ALL
select warehouse_code, partner_sku, sku, unit_code, condition_type_code, date_record, physical_qty, available_qty, pending_in_qty, pending_out_qty, 	
freeze_qty, 'LYB' as brand from `lyb_vietful_dwh.vietful_product_inventory_lyb`
UNION ALL
select warehouse_code, partner_sku, sku, unit_code, condition_type_code, date_record, physical_qty, available_qty, pending_in_qty, pending_out_qty, 	
freeze_qty, 'UME' as brand from `ume_vietful_dwh.vietful_product_inventory_lyb`
