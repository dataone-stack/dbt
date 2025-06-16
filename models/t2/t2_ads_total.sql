
SELECT
    DATE(date_start) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    spend,
    case
    when tk.ma_quan_ly IN ('LB000141')
    then COALESCE(
        CAST(
            JSON_VALUE(
                (
                    SELECT value
                    FROM UNNEST(action_values) AS value
                    WHERE JSON_VALUE(value, '$.action_type') = 'purchase'
                    LIMIT 1
                ),
                '$.value'
            ) AS FLOAT64
        ),
        0
    )
    else COALESCE(
        CAST(
            JSON_VALUE(
                (
                    SELECT value
                    FROM UNNEST(action_values) AS value
                    WHERE JSON_VALUE(value, '$.action_type') = 'onsite_conversion.purchase'
                    LIMIT 1
                ),
                '$.value'
            ) AS FLOAT64
        ),
        0
    )
    end AS doanhThuAds,
    'Facebook Ads' AS revenue_type,
    account_currency as currency
FROM {{ ref('t1_facebook_ads_total') }} fb 
left join {{ref("t1_tkqc")}} tk on cast(fb.account_id as string) = tk.idtkqc

UNION ALL

SELECT
    DATE(DATETIME_ADD(DATETIME(stat_time_day), INTERVAL 7 HOUR)) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    spend,
    CAST(total_onsite_shopping_value AS FLOAT64) AS doanhThuAds,
    'TikTok Ads' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_tiktok_ads_total') }}

UNION ALL

SELECT
    DATE(DATETIME_ADD(DATETIME(stat_time_day), INTERVAL 7 HOUR)) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    cost AS spend,
    gross_revenue AS doanhThuAds,
    'TikTok GMVmax' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_tiktokGMV_ads_total') }}

UNION ALL

SELECT
    DATE(date) AS date_start,
    CAST(idtkqc AS STRING) AS account_id,
    expense AS spend,
    broad_gmv AS doanhThuAds,
    'Shopee Ads' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_shopee_ads_total') }}

UNION ALL

SELECT
    DATE(date_start) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    chiphi AS spend,
    doanhThuAds AS doanhThuAds,
    'Shopee Search' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_shopee_search_ads_total') }}