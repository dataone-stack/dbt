-- SELECT * FROM `me_qa_phuong_facebook_ads_dwh.facebook_ads_ads_insights_default`
-- UNION ALL
-- SELECT * FROM `me_qa_minh_facebook_ads_dwh.facebook_ads_ads_insights_default` m
-- WHERE NOT EXISTS (
--   SELECT 1 FROM `me_qa_phuong_facebook_ads_dwh.facebook_ads_ads_insights_default` p
--   WHERE p.account_id = m.account_id
-- )


-- select * from `tai_nguyen_facebook_ads_dwh.facebook_ads_ads_insights_default`

select * from `ancung_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `chaching_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `ume_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `lyb_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `ancung_facebook_ads_dwh.facebook_ads_ads_insights_default`
union all
select * from `team_maxeagle_facebook_ads_3_dwh.facebook_ads_ads_insights_default`
union all
select * from `team_maxeagle_facebook_ads_2_dwh.facebook_ads_ads_insights_default` 
union all
select * from `team_maxeagle_facebook_ads_1_dwh.facebook_ads_ads_insights_default` 