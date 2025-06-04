SELECT 
    SAFE_CAST(t1.cpc AS FLOAT64) AS cpc,
    SAFE_CAST(t1.cpm AS FLOAT64) AS cpm,
    SAFE_CAST(t1.onsite_shopping AS INT64) AS onsite_shopping,
    SAFE_CAST(t1.total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
    SAFE_CAST(t1.spend AS INT64) AS spend,
    SAFE_CAST(t1.account_id AS STRING) AS account_id,
    SAFE_CAST(t1.stat_time_day AS DATE) AS stat_time_day,
    t2.ad_name,
    t2.ad_id,
    t1.clicks,
    t1.impressions
FROM tiktok_ads_dwh.chaching_tiktok_ads_dwh t1
LEFT JOIN tiktok_ads_dwh.chaching_tiktok_ads_dwh_ad t2 ON t1.ad_id = t2.ad_id
UNION ALL
SELECT 
    SAFE_CAST(t1.cpc AS FLOAT64) AS cpc,
    SAFE_CAST(t1.cpm AS FLOAT64) AS cpm,
    SAFE_CAST(t1.onsite_shopping AS INT64) AS onsite_shopping,
    SAFE_CAST(t1.total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
    SAFE_CAST(t1.spend AS INT64) AS spend,
    SAFE_CAST(t1.account_id AS STRING) AS account_id,
    SAFE_CAST(t1.stat_time_day AS DATE) AS stat_time_day,
    t2.ad_name,
    t2.ad_id,
    t1.clicks,
    t1.impressions
FROM tiktok_ads_dwh.lyb_tiktok_ads_dwh t1
LEFT JOIN tiktok_ads_dwh.lyb_tiktok_ads_dwh_ad t2 ON t1.ad_id = t2.ad_id
UNION ALL
SELECT 
    SAFE_CAST(t1.cpc AS FLOAT64) AS cpc,
    SAFE_CAST(t1.cpm AS FLOAT64) AS cpm,
    SAFE_CAST(t1.onsite_shopping AS INT64) AS onsite_shopping,
    SAFE_CAST(t1.total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
    SAFE_CAST(t1.spend AS INT64) AS spend,
    SAFE_CAST(t1.account_id AS STRING) AS account_id,
    SAFE_CAST(t1.stat_time_day AS DATE) AS stat_time_day,
    t2.ad_name,
    t2.ad_id,
    t1.clicks,
    t1.impressions
FROM tiktok_ads_dwh.ume_tiktok_ads_dwh t1
LEFT JOIN tiktok_ads_dwh.ume_tiktok_ads_dwh_ad t2 ON t1.ad_id = t2.ad_id
UNION ALL
SELECT 
    SAFE_CAST(t1.cpc AS FLOAT64) AS cpc,
    SAFE_CAST(t1.cpm AS FLOAT64) AS cpm,
    SAFE_CAST(t1.onsite_shopping AS INT64) AS onsite_shopping,
    SAFE_CAST(t1.total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
    SAFE_CAST(t1.spend AS INT64) AS spend,
    SAFE_CAST(t1.account_id AS STRING) AS account_id,
    SAFE_CAST(t1.stat_time_day AS DATE) AS stat_time_day,
    t2.ad_name,
    t2.ad_id,
    t1.clicks,
    t1.impressions
FROM tiktok_ads_dwh.ancung_tiktok_ads_dwh t1
LEFT JOIN tiktok_ads_dwh.ancung_tiktok_ads_dwh_ad t2 ON t1.ad_id = t2.ad_id
UNION ALL
SELECT 
    SAFE_CAST(t1.cpc AS FLOAT64) AS cpc,
    SAFE_CAST(t1.cpm AS FLOAT64) AS cpm,
    SAFE_CAST(t1.onsite_shopping AS INT64) AS onsite_shopping,
    SAFE_CAST(t1.total_onsite_shopping_value AS INT64) AS total_onsite_shopping_value,
    SAFE_CAST(t1.spend AS INT64) AS spend,
    SAFE_CAST(t1.account_id AS STRING) AS account_id,
    SAFE_CAST(t1.stat_time_day AS DATE) AS stat_time_day,
    t2.ad_name,
    t2.ad_id,
    t1.clicks,
    t1.impressions
FROM tiktok_ads_dwh.lybcosmetic_tiktok_ads_dwh t1
LEFT JOIN tiktok_ads_dwh.lybcosmetic_tiktok_ads_dwh_ad t2 ON t1.ad_id = t2.ad_id;