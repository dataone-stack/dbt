SELECT 
    ir_id,
    ir_code,
    warehouse_code,
    ir_status,
    ir_status_name,
    actual_arrival_date,
    finished_date,
    condition_type_code,
    partner_ir_code,
    ir_type,
    ir_type_name,
    or_id,
    or_code,
    country_code,
    currency_code,
    supplier_name,
    partner_branch_code,
    partner_address_book_code,
    created_date,
    brand,
    
    -- ============ THÔNG TIN TỪ GRNS (Goods Receipt Note) ============
    JSON_EXTRACT_SCALAR(grn_item, '$.code') AS grn_code,
    TIMESTAMP(JSON_EXTRACT_SCALAR(grn_item, '$.startTime')) AS grn_start_time,
    TIMESTAMP(JSON_EXTRACT_SCALAR(grn_item, '$.endTime')) AS grn_end_time,
    
    -- ============ CHI TIẾT SẢN PHẨM TRONG GRNS ============
    JSON_EXTRACT_SCALAR(grn_detail, '$.productName') AS product_name,
    JSON_EXTRACT_SCALAR(grn_detail, '$.sku') AS sku,
    JSON_EXTRACT_SCALAR(grn_detail, '$.partnerSKU') AS partnerSKU,
    JSON_EXTRACT_SCALAR(grn_detail, '$.unitCode') AS unitCode,
    JSON_EXTRACT_SCALAR(grn_detail, '$.unitName') AS unitName,
    JSON_EXTRACT_SCALAR(grn_detail, '$.conditionTypeCode') AS conditionTypeCode,
    JSON_EXTRACT_SCALAR(grn_detail, '$.conditionTypeName') AS conditionTypeName,
    CAST(JSON_EXTRACT_SCALAR(grn_detail, '$.qty') AS INT64) AS qty
    
FROM 
    {{ ref('t1_vietful_nhapkho_total') }},
    UNNEST(grns) AS grn_item,
    UNNEST(JSON_EXTRACT_ARRAY(grn_item, '$.details')) AS grn_detail
WHERE 
    grns IS NOT NULL
