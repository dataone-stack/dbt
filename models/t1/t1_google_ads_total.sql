with a as (
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_khoavu1211`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_buiducan541992_dwh`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_dekakhung541992`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_dekakhung5492`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_manh0888789863`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_adamgroup_reg8`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_vugiakhoa9797`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_ngotrangngan6688`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_adamgroup_reg9`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_dekakhung1992`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_umean1992`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_stct863`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,'VND' as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_vud95276`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_lyquocviet_one5`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_buithequoc97`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_agwuduhhxbxbnxnxkxkxk2`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_nguyenvuduca`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_ume_googleads_rukehajotiza10788`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_rosaliewalton892`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_hughjennefer`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_pl8582937`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_ngadenny522`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_quwakamara02104`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_tericaemerson`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_roartikali47897`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_caphemamxoi_googleads_mariegodfrey97790`
union all
select segments.date as segment_date,account_id,metrics.costMicros as spend,metrics.conversionsValue,customer.currencyCode as currency from `team_maxeagle_google_ads_dwh.me_be20_googleads_mailbiendongsoduqa01`
) 

select * from a where date(segment_date) <= '2025-09-31'
