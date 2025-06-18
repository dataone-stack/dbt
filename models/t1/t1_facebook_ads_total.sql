SELECT * FROM `me_qa_phuong_facebook_ads_dwh.facebook_ads_ads_insights_default`
UNION ALL
SELECT * FROM `me_qa_minh_facebook_ads_dwh.facebook_ads_ads_insights_default`
WHERE account_id NOT IN (
  SELECT account_id 
  FROM `me_qa_phuong_facebook_ads_dwh.facebook_ads_ads_insights_default`
)


--select * from `tai_nguyen_facebook_ads_dwh.facebook_ads_ads_insights_default`

-- select * from `ancung_facebook_ads_dwh.facebook_ads_ads_insights_default`
-- union all
-- select * from `chaching_facebook_ads_dwh.facebook_ads_ads_insights_default`
-- union all
-- select * from `ume_facebook_ads_dwh.facebook_ads_ads_insights_default`
-- union all
-- select * from `lyb_facebook_ads_dwh.facebook_ads_ads_insights_default`
-- union all
-- select * from `ancung_facebook_ads_dwh.facebook_ads_ads_insights_default`