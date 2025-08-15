select warehouse_code, partner_sku, sku, unit_code, condition_type_code, date_record, physical_qty, available_qty, pending_in_qty, pending_out_qty, 	
freeze_qty, last_updated_date, 'Chaching' as brand  from `chaching_vietful_dwh.vietful_product_inventory_lyb`
UNION ALL
select warehouse_code, partner_sku, sku, unit_code, condition_type_code, date_record, physical_qty, available_qty, pending_in_qty, pending_out_qty, 	
freeze_qty, last_updated_date, 'LYB' as brand from `lyb_vietful_dwh.vietful_product_inventory_lyb`
UNION ALL
select warehouse_code, partner_sku, sku, unit_code, condition_type_code, date_record, physical_qty, available_qty, pending_in_qty, pending_out_qty, 	
freeze_qty, last_updated_date, 'UME' as brand from `ume_vietful_dwh.vietful_product_inventory_lyb`
