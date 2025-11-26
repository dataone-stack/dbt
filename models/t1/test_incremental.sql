{{ config(
    materialized='incremental',
    partition_by={'field': 'date_start', 'data_type': 'date'},
    incremental_strategy='insert_overwrite'
) }}

with src as (

    -- ONE5
    select *, 'One5' as company
    from `team_a_tien_facebook_ads_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'One5' as company
    from `team_a_tien_facebook_ads_1_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    -- MAX EAGLE
    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_2_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_1_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_4_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_5_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_6_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_7_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_8_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

    union all
    select *, 'Max Eagle' as company
    from `team_maxeagle_facebook_ads_9_dwh.facebook_ads_ads_insights_default`
    where date_start >= date_sub(current_date(), interval 7 day)

)

select *
from src
