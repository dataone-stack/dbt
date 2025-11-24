with
    excluded_accounts as (
        select account_id
        from
            unnest(
                [
                    1315221749155592,
                    7293216250736969,
                    1195811448247540,
                    1056401632089228,
                    322182883718537,
                    817956517039104,
                    757805919618691,
                    937351694653423,
                    1180659216429445,
                    1555367388753773,
                    929702751880751,
                    423117176961512,
                    1074431423644303,
                    1570143330412431,
                    1628940277646511,
                    685890103495972,
                    1807488083011550,
                    17909020
                ]
            ) as account_id
    ),

    a as (
        select
            date(date_start) as date_start,
            cast(account_id as string) as account_id,
            account_name,
            ad_id,
            cast(campaign_id as string) as campaign_id,
            campaign_name,
            cast(spend as float64) as spend,
            case
                when tk.company = 'Max Eagle'
                then 0
                when
                    account_id in (select account_id from excluded_accounts)
                    and date(date_start) between '2025-09-19' and '2025-09-30'
                then 0
                else
                    coalesce(
                        cast(
                            json_value(
                                (
                                    select value
                                    from unnest(action_values) as value
                                    where
                                        json_value(value, '$.action_type') = case
                                            when objective = 'OUTCOME_SALES'
                                            then 'onsite_web_purchase'
                                            else 'onsite_conversion.purchase'
                                        end
                                    limit 1
                                ),
                                '$.value'
                            ) as float64
                        ),
                        0
                    )

            end as doanhthuads,

            'Facebook Ads' as revenue_type,
            account_currency as currency
        from {{ref("t1_facebook_ads_total")}} fb
        left join
            {{ref("t1_tkqc")}} tk
            on cast(fb.account_id as string) = tk.idtkqc

        union all

        select
            date_start,
            idtkqc as account_id,
            '-' as account_name,
            0 as ad_id,
            '-' as campaign_id,
            "" as campaign_name,

            cast(chiphiads as float64) as spend,
            0 as doanhthuads,
            'Facebook Ads' as revenue_type,
            '-' as currency
        from {{ref("t1_facebook_ads_voi_total")}}

        union all

        select
            date(
                datetime_add(datetime(a.stat_time_day), interval 7 hour)
            ) as date_start,
            cast(a.account_id as string) as account_id,
            account_name,
            a.ad_id,
            cast(b.campaign_id as string) as campaign_id,
            b.campaign_name as campaign_name,

            cast(a.spend as float64) as spend,
            cast(a.total_onsite_shopping_value as float64) as doanhthuads,
            'TikTok Ads' as revenue_type,
            '-' as currency
        from {{ref("t1_tiktok_ads_total")}} a
        left join
            {{ref("t1_tiktok_ads_ad_total")}} b
            on a.ad_id = b.ad_id

        union all

        select
            date(datetime_add(datetime(stat_time_day), interval 7 hour)) as date_start,
            cast(account_id as string) as account_id,
            account_name,
            0 as ad_id,
            '-' as campaign_id,
            "" as campaign_name,

            cast(cost as float64) as spend,
            gross_revenue as doanhthuads,
            'TikTok GMVmax' as revenue_type,
            case
                when
                    cast(account_id as string) in (
                        '7450033634720874512',
                        '7531919757827080209',
                        '7441124535434280976',
                        '762021588532155',
                        '7490628408758910993',
                        '7509839186493472785'
                    )
                then 'USD'
                else '-'
            end as currency
        from {{ref("t1_tiktokGMV_ads_total")}}

        union all

        select
            date(date) as date_start,
            cast(idtkqc as string) as account_id,
            '-' as account_name,
            0 as ad_id,
            '-' as campaign_id,
            "" as campaign_name,
            cast(expense as float64) as spend,
            broad_gmv as doanhthuads,
            'Shopee Ads' as revenue_type,
            '-' as currency
        from {{ref("t1_shopee_ads_total")}}

        union all

        select
            date(date_start) as date_start,
            cast(account_id as string) as account_id,
            '-' as account_name,
            0 as ad_id,
            '-' campaign_id,
            "" as campaign_name,
            cast(chiphi as float64) as spend,
            doanhthuads as doanhthuads,
            'Shopee Search' as revenue_type,
            '-' as currency
        from {{ref("t1_shopee_search_ads_total")}}

        union all

        select
            segment_date as date_start,
            cast(account_id as string) as account_id,
            '-' as account_name,
            0 as ad_id,
            '-' as campaign_id,
            "" as campaign_name,
            cast(safe_divide(spend, 1000000) as float64) as spend,
            0 as doanhthuads,
            'Google Ads' as revenue_type,
            currency
        from {{ref("t1_google_ads_total")}}

        union all
        select
            date_start,
            cast(idtkqc as string) as account_id,
            '-' as account_name,
            0 as ad_id,
            '-' as campaign_id,
            "" as campaign_name,
            chiphi as spend,
            0 as doanhthuads,
            'Google Ads' as revenue_type,
            'VND' as currency
        from `google_sheet.me_gg_tuminh`

        union all

        select
            date_start,
            cast(idtkqc as string) as account_id,
            '-' as account_name,
            0 as ad_id,
            '-' as campaign_id,
            "" as campaign_name,
            case
                when cast(idtkqc as string) in ('-')  -- '1271178195','4142173605','9451868664')
                then cast(chiphi as float64)
                else cast(chiphi * 26700 as float64)
            end as spend,
            0 as doanhthuads,
            'Google Ads' as revenue_type,
            '-' as currency
        from {{ref("t1_google_ads_nguong_total")}}

        union all

        select
            date(date_start) as date_start,
            cast(idtkqc as string) as account_id,
            '-' as account_name,
            0 as ad_id,
            '-' as campaign_id,
            "" as campaign_name,
            case when tien_te = 'USD' then spend * 26700 else spend end as spend,
            0 as doanhthuads,
            'Google Ads' as revenue_type,
            tien_te
        from `google_sheet.buiducan_google_ads`
    )

select
    date_start,
    account_id,
    account_name,
    ad_id,
    campaign_id,
    campaign_name,

    case
        when
            account_id in (
                '7450033634720874512',
                '7531919757827080209',
                '7441124535434280976',
                '762021588532155',
                '7490628408758910993',
                '7509839186493472785',
                '1659671047971764',
                '3254199458069531'
            )
        then spend * b.rate
        else spend
    end as spend,

    case
        when
            account_id in (
                '7450033634720874512',
                '7531919757827080209',
                '7441124535434280976',
                '7490628408758910993',
                '7509839186493472785',
                '1659671047971764',
                '3254199458069531'
            )
        then doanhthuads * b.rate
        else doanhthuads
    end as doanhthuads,

    revenue_type,

    currency
from a
left join
    `pushsale_maxeagle_dwh.maxeagle_currency_rates` b on a.currency = b.currency_code
