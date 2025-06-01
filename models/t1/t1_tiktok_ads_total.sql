SELECT 
  SAFE_CAST(cpc AS FLOAT64) AS cpc,
  SAFE_CAST(cpm AS FLOAT64) AS cpm,
  SAFE_CAST(onsite_shopping AS INT64) AS onsite_shopping,
  SAFE_CAST(total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
  SAFE_CAST(spend AS INT64) AS spend,
  SAFE_CAST(account_id AS STRING) AS account_id,
  SAFE_CAST(stat_time_day AS DATE) AS stat_time_day
FROM tiktok_ads_dwh.chaching_tiktok_ads_dwh

UNION ALL

SELECT 
  SAFE_CAST(cpc AS FLOAT64) AS cpc,
  SAFE_CAST(cpm AS FLOAT64) AS cpm,
  SAFE_CAST(onsite_shopping AS INT64) AS onsite_shopping,
  SAFE_CAST(total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
  SAFE_CAST(spend AS INT64) AS spend,
  SAFE_CAST(account_id AS STRING) AS account_id,
  SAFE_CAST(stat_time_day AS DATE) AS stat_time_day
FROM tiktok_ads_dwh.lyb_tiktok_ads_dwh

UNION ALL

SELECT 
  SAFE_CAST(cpc AS FLOAT64) AS cpc,
  SAFE_CAST(cpm AS FLOAT64) AS cpm,
  SAFE_CAST(onsite_shopping AS INT64) AS onsite_shopping,
  SAFE_CAST(total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
  SAFE_CAST(spend AS INT64) AS spend,
  SAFE_CAST(account_id AS STRING) AS account_id,
  SAFE_CAST(stat_time_day AS DATE) AS stat_time_day
FROM tiktok_ads_dwh.ume_tiktok_ads_dwh

UNION ALL

SELECT 
  SAFE_CAST(cpc AS FLOAT64) AS cpc,
  SAFE_CAST(cpm AS FLOAT64) AS cpm,
  SAFE_CAST(onsite_shopping AS INT64) AS onsite_shopping,
  SAFE_CAST(total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
  SAFE_CAST(spend AS INT64) AS spend,
  SAFE_CAST(account_id AS STRING) AS account_id,
  SAFE_CAST(stat_time_day AS DATE) AS stat_time_day
FROM tiktok_ads_dwh.ancung_tiktok_ads_dwh

UNION ALL

SELECT 
  SAFE_CAST(cpc AS FLOAT64) AS cpc,
  SAFE_CAST(cpm AS FLOAT64) AS cpm,
  SAFE_CAST(onsite_shopping AS INT64) AS onsite_shopping,
  SAFE_CAST(total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
  SAFE_CAST(spend AS INT64) AS spend,
  SAFE_CAST(account_id AS STRING) AS account_id,
  SAFE_CAST(stat_time_day AS DATE) AS stat_time_day
FROM tiktok_ads_dwh.lybcosmetic_tiktok_ads_dwh
