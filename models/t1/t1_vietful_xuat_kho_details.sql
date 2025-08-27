SELECT 
    or_id,
    or_code,
    JSON_EXTRACT_SCALAR(detail_item, '$.partnerSKU') AS partnerSKU,
    JSON_EXTRACT_SCALAR(detail_item, '$.sku') AS sku,
    JSON_EXTRACT_SCALAR(detail_item, '$.unitCode') AS unitCode,
    JSON_EXTRACT_SCALAR(detail_item, '$.conditionTypeCode') AS conditionTypeCode,
    CAST(JSON_EXTRACT_SCALAR(detail_item, '$.orderQty') AS INT64) AS orderQty,
    CAST(JSON_EXTRACT_SCALAR(detail_item, '$.packedQty') AS INT64) AS packedQty,
    JSON_EXTRACT_SCALAR(detail_item, '$.serials') AS serials,
    JSON_EXTRACT_SCALAR(detail_item, '$.note') AS note,
    CAST(JSON_EXTRACT_SCALAR(detail_item, '$.price') AS FLOAT64) AS price,
    CAST(JSON_EXTRACT_SCALAR(detail_item, '$.discountValue') AS FLOAT64) AS discountValue,
    CAST(JSON_EXTRACT_SCALAR(detail_item, '$.paymentAmount') AS FLOAT64) AS paymentAmount,
    JSON_EXTRACT_SCALAR(detail_item, '$.categoryCode') AS categoryCode,
    JSON_EXTRACT_SCALAR(detail_item, '$.categoryName') AS categoryName
FROM 
    {{ ref('t1_vietful_xuatkho_total') }},
    UNNEST(details) AS detail_item
WHERE details IS NOT NULL
