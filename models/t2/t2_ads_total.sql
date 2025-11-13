

WITH excluded_accounts AS (
  SELECT account_id FROM UNNEST([
    1315221749155592, 7293216250736969, 1195811448247540, 1056401632089228,
    322182883718537, 817956517039104, 757805919618691, 937351694653423,
    1180659216429445, 1555367388753773, 929702751880751,423117176961512,1074431423644303,1570143330412431,1628940277646511,685890103495972,1807488083011550,
    17909020
  ]) AS account_id
),

 a as (
SELECT
    DATE(date_start) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    account_name,
    ad_id,
    cast(campaign_id as string) as campaign_id,
    campaign_name,
    cast(spend as float64) as spend,
    CASE
        when tk.company = 'Max Eagle' 
        then 0
        WHEN account_id IN (SELECT account_id FROM excluded_accounts)
             AND DATE(date_start) BETWEEN '2025-09-19' AND '2025-09-30' THEN 0
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
    date_start,
    idtkqc as account_id,
    '-' as account_name,
    0 as ad_id,
    '-' as campaign_id,
    "" as campaign_name,

    cast(chiphiads as float64) AS spend,
    0 AS doanhThuAds,
    'Facebook Ads' AS revenue_type,
    '-' as currency
FROM {{ref("t1_facebook_ads_voi_total")}}

UNION ALL

SELECT
    DATE(DATETIME_ADD(DATETIME(a.stat_time_day), INTERVAL 7 HOUR)) AS date_start,
    CAST(a.account_id AS STRING) AS account_id,
    account_name,
    a.ad_id,
    cast(b.campaign_id as string) as campaign_id,
    b.campaign_name as campaign_name,
    
    cast(a.spend as float64) as spend,
    CAST(a.total_onsite_shopping_value AS FLOAT64) AS doanhThuAds,
    'TikTok Ads' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_tiktok_ads_total') }} a left join {{ref("t1_tiktok_ads_ad_total")}} b on a.ad_id = b.ad_id

UNION ALL

SELECT
    DATE(DATETIME_ADD(DATETIME(stat_time_day), INTERVAL 7 HOUR)) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    account_name,
    0 as ad_id,
    '-' as campaign_id,
    "" as campaign_name,

    cast(cost as float64) AS spend,
    gross_revenue AS doanhThuAds,
    'TikTok GMVmax' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_tiktokGMV_ads_total') }}

UNION ALL

SELECT
    DATE(date) AS date_start,
    CAST(idtkqc AS STRING) AS account_id,
    '-' as account_name,
    0 as ad_id,
    '-' as campaign_id,
    "" as campaign_name,
    cast(expense as float64) AS spend,
    broad_gmv AS doanhThuAds,
    'Shopee Ads' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_shopee_ads_total') }}

UNION ALL

SELECT
    DATE(date_start) AS date_start,
    CAST(account_id AS STRING) AS account_id,
    '-' as account_name,
    0 as ad_id,
    '-' campaign_id,
    "" as campaign_name,
    cast(chiphi as float64) AS spend,
    doanhThuAds AS doanhThuAds,
    'Shopee Search' AS revenue_type,
    '-' as currency
FROM {{ ref('t1_shopee_search_ads_total') }}


union all

select 
  segment_date as date_start,
  CAST(account_id AS STRING) AS account_id,
  '-' as account_name,
  0 as ad_id,
  '-' as campaign_id,
  "" as campaign_name,
  cast (safe_divide(spend,1000000) as float64)  as spend,
  0 as doanhThuAds,
  'Google Ads' as revenue_type,
  currency
from {{ref("t1_google_ads_total")}} 

union all
select 
  date_start,
  CAST(idtkqc AS STRING) AS account_id,
  '-' as account_name,
  0 as ad_id,
  '-' as campaign_id,
  "" as campaign_name,
  chiphi as spend,
  0 as doanhThuAds,
  'Google Ads' as revenue_type,
  'VND' as currency
from `google_sheet.me_gg_tuminh`

union all

select 
  date_start,
  cast(idtkqc as string) AS account_id,
  '-' as account_name,
  0 as ad_id,
  '-' as campaign_id,
  "" as campaign_name,
  case
  when cast(idtkqc as string) in ('1271178195','4142173605','9451868664')
  then cast(chiPhi as float64)
  else cast(chiPhi * 26700 as float64)
  end as spend,
  0 as doanhThuAds,
  'Google Ads' as revenue_type,
  '-' AS currency
from {{ref("t1_google_ads_nguong_total")}}

union all

select 
date(date_start) as date_start,
cast (idtkqc as string) as account_id,
'-' as account_name,
0 as ad_id,
'-' as campaign_id,
"" as campaign_name,
case
    when tien_te = 'USD'
    then spend * 26700
    else spend
end as spend,
0 as doanhThuAds,
  'Google Ads' as revenue_type,
  tien_te
from `google_sheet.buiducan_google_ads`
)

select 
    date_start,
    account_id,
    account_name,
    ad_id,
    campaign_id,
    campaign_name,

    case
        when account_id in ('7450033634720874512','7531919757827080209','7441124535434280976','762021588532155','7490628408758910993','7509839186493472785')
        then spend * 26700
        else spend
    end as spend,

    case
        when account_id in ('7450033634720874512','7531919757827080209','7441124535434280976','7490628408758910993','7509839186493472785')
        then doanhThuAds * 26700
        else doanhThuAds
    end as doanhThuAds,

    revenue_type,

    currency
from a
    