SELECT 
  inve.warehouse_code,
  inve.partner_sku,
  inve.sku,
  pro.product_name,
  inve.unit_code,
  inve.condition_type_code,
  inve.date_record,
  inve.physical_qty,
  inve.available_qty,
  inve.pending_in_qty,
  inve.pending_out_qty,
  inve.freeze_qty
FROM 
  {{ ref('t1_vietful_product_inventory') }} inve
  LEFT JOIN {{ ref('t1_vietful_product_total') }} pro
  ON pro.sku = inve.sku
