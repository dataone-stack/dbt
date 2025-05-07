SELECT date_start as stat_time_date,account_id,spend FROM  {{ ref('t1_facebook_ads_cost')}}
union all
select stat_time_date,account_id,spend From {{ref("t1_tiktok_ads_cost")}}
union all
select stat_time_date,account_id,spend From {{ref("t1_tiktokGMV_ads_cost")}}
