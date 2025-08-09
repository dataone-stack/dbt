
SELECT
    DATE(date_start) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    ad_id,
    campaign_id,
    campaign_name,
    spend,
    CASE
        when tk.company = 'Max Eagle'
        then 0
        else COALESCE(
        CAST(
            JSON_VALUE(
                (
                    SELECT value
                    FROM UNNEST(action_values) AS value
                    WHERE JSON_VALUE(value, '$.action_type') = 
                        CASE 
                            WHEN objective = 'OUTCOME_SALES' THEN 'onsite_web_purchase'
                            ELSE 'onsite_conversion.purchase'
                        END
                    LIMIT 1
                ),
                '$.value'
            ) AS FLOAT64
        ),
        0
    )

    end as doanhThuAds,

    'Facebook Ads' AS revenue_type,
    account_currency as currency
FROM {{ ref('t1_facebook_ads_total') }} fb 
LEFT JOIN {{ref("t1_tkqc")}} tk ON cast(fb.account_id as string) = tk.idtkqc

UNION ALL

SELECT
    DATE(DATETIME_ADD(DATETIME(stat_time_day), INTERVAL 7 HOUR)) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    ad_id,
    0 as campaign_id,
    "" as campaign_name,
    spend,
    CAST(total_onsite_shopping_value AS FLOAT64) AS doanhThuAds,
    'TikTok Ads' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_tiktok_ads_total') }}

UNION ALL

SELECT
    DATE(DATETIME_ADD(DATETIME(stat_time_day), INTERVAL 7 HOUR)) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    0 as ad_id,
    0 as campaign_id,
    "" as campaign_name,
    cost AS spend,
    gross_revenue AS doanhThuAds,
    'TikTok GMVmax' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_tiktokGMV_ads_total') }}

UNION ALL

SELECT
    DATE(date) AS date_start,
    CAST(idtkqc AS STRING) AS account_id,
    0 as ad_id,
    0 as campaign_id,
    "" as campaign_name,
    expense AS spend,
    broad_gmv AS doanhThuAds,
    'Shopee Ads' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_shopee_ads_total') }}

UNION ALL

SELECT
    DATE(date_start) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    0 as ad_id,
    0 campaign_id,
    "" as campaign_name,
    chiphi AS spend,
    doanhThuAds AS doanhThuAds,
    'Shopee Search' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_shopee_search_ads_total') }}


union all

select 
  segments_date as date_start,
  CAST(account_id AS STRING) AS account_id,
  0 as ad_id,
  0 as campaign_id,
  "" as campaign_name,
  cast (safe_divide(metrics.costMicros,1000000) as float64)  as spend,
  0 as doanhThuAds,
  'Google Ads' as revenue_type,
  customer.currencyCode as currency
from {{ref("t1_google_ads_total")}}