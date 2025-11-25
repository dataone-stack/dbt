with a as(
SELECT
  t3.*,
  CASE
    WHEN t1.main_partner_sku IS NOT NULL THEN (
      SELECT TO_JSON_STRING(ARRAY_AGG(
        STRUCT(
            sku,
            product_name,
            qty
        )
      ))
      FROM `dtm.t1_vietful_combo_details` t1sub
      WHERE t1sub.main_partner_sku = t3.sku_code
    )
    ELSE NULL
  END AS combo_details_json
FROM `dtm.t3_revenue_all_channel` t3
LEFT JOIN (
  SELECT DISTINCT main_partner_sku FROM `dtm.t1_vietful_combo_details`
) t1 ON t1.main_partner_sku = t3.sku_code
)

select * from a --where combo_details_json is not null  limit 10
