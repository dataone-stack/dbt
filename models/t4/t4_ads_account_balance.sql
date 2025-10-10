select
        ads.*, 
        account.balance, 
        account.amount_spent,
        case
            when account.balance is not null then "balance"
            else ""
        end as balance_status
    
    from {{ref("t3_ads_total_with_tkqc")}} as ads
    left join {{ref("t1_ads_account_facebook")}} as account
        on cast(account.account_id as STRING) = ads.idtkqc
        and date(account.elton_record_date) = ads.date_start