SELECT
    DATE(date_start) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    spend,
    COALESCE(
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
    ) AS doanhThuAds
FROM {{ ref('t1_facebook_ads_total') }}

UNION ALL

SELECT
    DATE(stat_time_day) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    spend,
    CAST(total_onsite_shopping_value AS FLOAT64) AS doanhThuAds
FROM {{ ref('t1_tiktok_ads_total') }}

UNION ALL

SELECT
    DATE(stat_time_day) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    spend,
    0 AS doanhThuAds
FROM {{ ref('t1_tiktokGMV_ads_total') }}

UNION ALL

SELECT
    DATE(date) AS date_start,
    CAST(idtkqc AS STRING) AS account_id,
    expense AS spend,
    broad_gmv AS doanhThuAds
FROM {{ ref('t1_shopee_ads_total') }}
