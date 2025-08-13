WITH ads_total_with_tkqc AS (
    SELECT
        ads.date_start,
        ads.revenue_type,
        -- Ưu tiên thông tin từ campaign mapping, fallback về tkqc
        COALESCE(campaign_team.staff_code, tkqc.ma_nhan_vien) as ma_nhan_vien,
        COALESCE(campaign_team.staff, tkqc.staff) as staff,
        COALESCE(campaign_team.manager_code, tkqc.ma_quan_ly) as ma_quan_ly,
        COALESCE(campaign_team.manager, tkqc.manager) as manager,
        tkqc.idtkqc,
        tkqc.nametkqc,
        tkqc.brand,
        tkqc.channel,
        ads.currency,
        tkqc.company,
        tkqc.ben_thue,
        tkqc.dau_the,
        MAX(tkqc.phi_thue) as phi_thue,
        SUM(ads.spend) AS chiPhiAds,
        SUM(ads.doanhThuAds) AS doanhThuAds,
        SUM(ads.spend) * (1 + COALESCE(MAX(tkqc.phi_thue), 0)) as chi_phi_agency
    FROM (
        -- Loại bỏ trùng lặp trong t2_ads_total trước khi JOIN
        SELECT DISTINCT
            date_start,
            revenue_type,
            account_id,
            ad_id,
            campaign_id,
            currency,
            spend,
            doanhThuAds
        FROM {{ ref('t2_ads_total') }}
    ) AS ads
    
    -- LEFT JOIN với campaign team mapping trước
    LEFT JOIN {{ ref('t1_ads_campaign_by_team') }} AS campaign_team
        ON CAST(ads.campaign_id AS STRING) = CAST(campaign_team.campaign_id AS STRING)
    
    -- RIGHT JOIN với tkqc như cũ
    RIGHT JOIN {{ ref('t2_tkqc_total') }} AS tkqc
        ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
        AND DATE(ads.date_start) >= DATE(tkqc.start_date)
        AND (tkqc.end_date IS NULL OR DATE(ads.date_start) <= DATE(tkqc.end_date))
        
    GROUP BY
        ads.date_start,
        tkqc.idtkqc,
        tkqc.nametkqc,
        COALESCE(campaign_team.staff_code, tkqc.ma_nhan_vien),
        COALESCE(campaign_team.staff, tkqc.staff),
        COALESCE(campaign_team.manager, tkqc.manager),
        COALESCE(campaign_team.manager_code, tkqc.ma_quan_ly),
        tkqc.brand,
        tkqc.channel,
        ads.revenue_type,
        ads.currency,
        tkqc.company,
        tkqc.ben_thue,
        tkqc.phi_thue,
        tkqc.dau_the
)

SELECT 
    ads.idtkqc,
    ads.nametkqc,
    ads.ma_nhan_vien,
    ads.staff,
    ads.manager,
    ads.ma_quan_ly,
    ads.ben_thue,
    ads.brand,
    ads.company,
    EXTRACT(YEAR FROM ads.date_start) as year,
    EXTRACT(MONTH FROM ads.date_start) as month,
    SUM(ads.chiPhiAds) as chi_phi_ads,
    max(invoice.spend) as spend,
    max(invoice.invoice) as invoice,
    max(invoice.amount) as amount
FROM 
    ads_total_with_tkqc ads
    Full Outer JOIN {{ ref('t1_financial_invoice_ads') }} AS invoice 
    ON ads.idtkqc  = invoice.account_ads_id and EXTRACT(MONTH from ads.date_start) = invoice.month
    GROUP BY
       EXTRACT(YEAR FROM ads.date_start), 
        EXTRACT(MONTH FROM ads.date_start),
        ads.idtkqc, 
        ads.nametkqc,
        ads.ma_nhan_vien, 
        ads.manager, 
        ads.staff, 
        ads.ma_quan_ly, 
        ads.ben_thue, 
        ads.brand, 
        ads.company


