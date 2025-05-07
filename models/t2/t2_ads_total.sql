SELECT date(date_start) as date_start,account_id,spend FROM  {{ ref('t1_facebook_ads_cost')}}
union all
select date(stat_time_date) as date_start ,account_id,spend From {{ref("t1_tiktok_ads_cost")}}
union all
select date(stat_time_date) as date_start ,account_id,spend From {{ref("t1_tiktokGMV_ads_cost")}}
