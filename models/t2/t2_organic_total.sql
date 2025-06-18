with pos_total as(
select date(datetime_add(inserted_at, INTERVAL 7 hour)) as date_insert, 'Facebook' as channel,brand, sum(total_price_after_sub_discount) as doanhthu
from {{ref("t1_pancake_pos_order_total")}}
where order_sources_name in ('Facebook') and marketer is null and brand != 'UME'
group by brand,date(datetime_add(inserted_at, INTERVAL 7 hour))
),

doanh_thu_ads_total as (
  SELECT
    DATE(date_start) AS date_start,
    tk.channel,
    tk.brand,
    sum(
    COALESCE(
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
        )) AS doanhThuAds
FROM {{ref("t1_facebook_ads_total")}} fb 
LEFT JOIN {{ref("t1_tkqc")}} tk ON cast(fb.account_id as string) = tk.idtkqc
group by  
    date_start,
    tk.channel,
    tk.brand
),

organic as (
select
  ads.date_start,
  ads.channel,
  ads.brand,
  pos.doanhthu - ads.doanhThuAds as doanhThuOrganic
from pos_total pos 
left join doanh_thu_ads_total ads on pos.date_insert = ads.date_start and pos.channel = ads.channel and pos.brand = ads.brand
)

  SELECT
    DATE(adstot.date_start) AS date_start,
    tk.channel,
    tk.brand,
    tk.ma_nhan_vien,
    tk.ma_quan_ly,
    tk.staff,
    tk.manager,
    CAST(
        SAFE_DIVIDE(
            SUM(
                COALESCE(
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
                )
            ) * SUM(org.doanhThuOrganic),
            SUM(adstot.doanhThuAds)
        ) AS INT64
    ) AS doanhThuOrganic
  FROM {{ref("t1_facebook_ads_total")}} fb 
  LEFT JOIN {{ref("t1_tkqc")}} tk ON cast(fb.account_id as string) = tk.idtkqc
  left join doanh_thu_ads_total adstot on fb.date_start =  adstot.date_start and tk.brand = adstot.brand and tk.channel = adstot.channel
  left join organic org on fb.date_start =  org.date_start and tk.brand = org.brand and tk.channel = org.channel
  group by  
      adstot.date_start,
      tk.channel,
      tk.brand,
      tk.ma_nhan_vien,
      tk.ma_quan_ly,
      tk.staff,
      tk.manager
      





