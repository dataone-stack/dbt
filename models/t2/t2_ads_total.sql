SELECT date(date_start) as date_start,account_id,spend,COALESCE(
    CAST(
      JSON_VALUE(
        (
          SELECT value
          FROM UNNEST(action_values) AS value
          WHERE JSON_VALUE(value, '$.action_type') = 'onsite_conversion.purchase'
          LIMIT 1
        ),
        '$.value'
      ) AS FLOAT64
    ),
    0
  ) AS doanhThuAds FROM  {{ ref('t1_facebook_ads_total')}}
union all
select date(stat_time_day) as date_start ,account_id,spend,cast(total_onsite_shopping_value as FLOAT64) as doanhThuAds From {{ref("t1_tiktok_ads_total")}}
-- union all
-- select date(stat_time_day) as date_start ,account_id,spend, 0 as doanhThuAds From {{ref("t1_tiktokGMV_ads_total")}}
union all
select date(date) as date_start , cast(idtkqc as int64) as account_id,expense as spend, broad_gmv as doanhThuAds From {{ref("t1_shopee_ads_total")}}l