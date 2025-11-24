WITH unnested_bundles AS (
  SELECT 
    product_id,
    product_name,
    sku AS main_sku,
    partner_sku AS main_partner_sku,
    bundle_json,
    brand
  FROM 
    `dtm.t1_vietful_product_total`,
    UNNEST(product_bundles) AS bundle_json
),
parsed_bundles AS (
  SELECT
    product_id,
    product_name,
    main_sku,
    main_partner_sku,
    brand,
    JSON_EXTRACT_SCALAR(bundle_json, '$.sku') AS bundle_sku,
    JSON_EXTRACT_SCALAR(bundle_json, '$.unitCode') AS unit_code,
    CAST(JSON_EXTRACT_SCALAR(bundle_json, '$.qty') AS INT64) AS qty
  FROM 
    unnested_bundles
)
SELECT 
  pb.product_id,
  pb.main_sku,
  pb.main_partner_sku,
  pb.product_name,
  pb.bundle_sku AS sku,
  p2.partner_sku,
  pb.qty,
  pb.brand as warehouse_name
FROM 
  parsed_bundles pb
LEFT JOIN 
  `dtm.t1_vietful_product_total` p2
  ON pb.bundle_sku = p2.sku and pb.brand = p2.brand








