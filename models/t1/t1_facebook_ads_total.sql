-- SELECT * FROM `me_qa_phuong_facebook_ads_dwh.facebook_ads_ads_insights_default`
-- UNION ALL
-- SELECT * FROM `me_qa_minh_facebook_ads_dwh.facebook_ads_ads_insights_default` m
-- WHERE NOT EXISTS (
--   SELECT 1 FROM `me_qa_phuong_facebook_ads_dwh.facebook_ads_ads_insights_default` p
--   WHERE p.account_id = m.account_id
-- )


select * from `ancung_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `chaching_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `ume_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `lyb_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
-- select * from `ancung_facebook_ads_dwh.facebook_ads_ads_insights_default`
-- union all
-- select * from `team_maxeagle_facebook_ads_3_dwh.facebook_ads_ads_insights_default`
-- union all
select * from `team_maxeagle_facebook_ads_2_dwh.facebook_ads_ads_insights_default` 
union all
select * from `team_maxeagle_facebook_ads_1_dwh.facebook_ads_ads_insights_default`
union all
select * from `team_maxeagle_facebook_ads_3_dwh.facebook_ads_ads_insights_default`






-- WITH combined_data AS (
--   -- Phần dữ liệu giữ nguyên
--   SELECT * FROM `ancung_facebook_ads_dwh.facebook_ads_ads_insights_default`
--   UNION ALL
--   SELECT * FROM `chaching_facebook_ads_dwh.facebook_ads_ads_insights_default`
--   UNION ALL
--   SELECT * FROM `ume_facebook_ads_dwh.facebook_ads_ads_insights_default`
--   UNION ALL
--   SELECT * FROM `lyb_facebook_ads_dwh.facebook_ads_ads_insights_default`
  
--   UNION ALL
  
--   -- Dữ liệu tháng 5 CHỈ từ crypto-arcade (loại trừ team_maxeagle)
--   SELECT * FROM `crypto-arcade-453509-i8.dtm.facebook_ads_qa`
--   WHERE EXTRACT(MONTH FROM date_start) = 5
  
--   UNION ALL
  
--   -- Dữ liệu từ tháng 6 trở đi từ team_maxeagle (loại trừ tháng 5)
--   SELECT * FROM `team_maxeagle_facebook_ads_3_dwh.facebook_ads_ads_insights_default`
--   WHERE EXTRACT(MONTH FROM date_start) >= 6
--   UNION ALL
--   SELECT * FROM `team_maxeagle_facebook_ads_2_dwh.facebook_ads_ads_insights_default`
--   WHERE EXTRACT(MONTH FROM date_start) >= 6
--   UNION ALL
--   SELECT * FROM `team_maxeagle_facebook_ads_1_dwh.facebook_ads_ads_insights_default`
--   WHERE EXTRACT(MONTH FROM date_start) >= 6
-- )

-- SELECT * FROM combined_data
-- ORDER BY date_start