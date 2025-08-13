SELECT * FROM {{ ref('t3_one5_kpi_ads_total') }}

UNION ALL

SELECT * FROM {{ ref('t3_me_kpi_ads_total') }}