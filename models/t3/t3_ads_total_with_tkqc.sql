WITH ads_total_with_tkqc AS (
    SELECT 
        ads.date_start,
        tkqc.idtkqc,
        tkqc.nametkqc,
        tkqc.ma_nhan_vien,
        tkqc.staff,
        tkqc.manager, 
        tkqc.brand,
        tkqc.channel,
        SUM(ads.spend) AS chiPhiAds, 
        SUM(ads.doanhThuAds) AS doanhThuAds
    FROM {{ ref('t2_ads_total')}} AS ads 
    RIGHT JOIN {{ ref('t1_tkqc') }} AS tkqc
        ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
    GROUP BY 
        ads.date_start,
        tkqc.idtkqc,
        tkqc.nametkqc,
        tkqc.ma_nhan_vien,
        tkqc.staff,
        tkqc.manager, 
        tkqc.brand,
        tkqc.channel
),
ads_ladipageFacebook_total_with_tkqc AS (
    SELECT 
        ads.*,
        CASE 
            WHEN ROW_NUMBER() OVER (
                PARTITION BY ads.date_start, ads.ma_nhan_vien, ads.manager, ads.brand, ads.channel 
                ORDER BY ladi.date_insert
            ) = 1 THEN ladi.doanhThuLadi 
            ELSE 0 
        END AS doanhThuLadi
    FROM ads_total_with_tkqc AS ads
    LEFT JOIN {{ ref('t2_ladipage_facebook_total') }} AS ladi
        ON ads.date_start = ladi.date_insert 
        AND ads.ma_nhan_vien = ladi.id_staff 
        AND ads.manager = ladi.manager 
        AND ads.brand = ladi.brand 
        AND ads.channel = ladi.channel
)

SELECT
    ads.*,
    gmv.doanhThuAds AS doanhThuGMVTiktok
FROM ads_ladipageFacebook_total_with_tkqc AS ads 
LEFT JOIN {{ref("t1_tiktokLive_ads_doanhThu_total")}} AS gmv 
    ON gmv.date_start = ads.date_start 
    AND CAST(gmv.account_id AS STRING) = cast (ads.idtkqc as string )
