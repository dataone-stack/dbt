-- {{ config(
--     materialized='incremental',
--     unique_key=['company','account_id', 'ad_id', 'date_start'],
--     incremental_strategy='merge'
-- ) }}

-- WITH unioned AS (

--     SELECT *, 'One5' AS company
--     FROM `team_a_tien_facebook_ads_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'One5' AS company
--     FROM `team_a_tien_facebook_ads_1_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_2_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_1_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_4_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_5_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_6_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_7_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_8_dwh.facebook_ads_ads_insights_default`

--     UNION ALL
--     SELECT *, 'Max Eagle' AS company
--     FROM `team_maxeagle_facebook_ads_9_dwh.facebook_ads_ads_insights_default`
-- )

-- SELECT *
-- FROM unioned

-- {% if is_incremental() %}
-- WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
-- {% endif %}
