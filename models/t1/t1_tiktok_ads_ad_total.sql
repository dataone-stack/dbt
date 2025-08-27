select account_id, ad_id,campaign_id,campaign_name,ad_name from `team_a_tien_tiktok_ads_dwh.team_a_tien_tiktok_ads_ad`

union all

select account_id, ad_id,campaign_id,campaign_name,ad_name from `team_maxeagle_tiktok_ads_dwh.team_maxeagle_tiktok_ads_ad`