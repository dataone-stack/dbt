
WITH ads_total_with_tkqc AS (
    SELECT
        ads.date_start,
        ads.revenue_type,
        tkqc.idtkqc,
        tkqc.nametkqc,
        tkqc.ma_nhan_vien,
        tkqc.staff,
        tkqc.manager,
        tkqc.ma_quan_ly,
        tkqc.brand,
        tkqc.channel,
        ads.currency,
        tkqc.company,
        tkqc.ben_thue,
        tkqc.dau_the,
        tkqc.company,
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
            currency,
            spend,
            doanhThuAds
        FROM {{ ref('t2_ads_total') }}
    ) AS ads
    RIGHT JOIN {{ ref('t2_tkqc_total') }} AS tkqc
        ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
        AND DATE(ads.date_start) >= DATE(tkqc.start_date)
        AND (tkqc.end_date IS NULL OR DATE(ads.date_start) <= DATE(tkqc.end_date))
    GROUP BY
        ads.date_start,
        tkqc.idtkqc,
        tkqc.nametkqc,
        tkqc.ma_nhan_vien,
        tkqc.staff,
        tkqc.manager,
        tkqc.ma_quan_ly,
        tkqc.brand,
        tkqc.channel,
        ads.revenue_type,
        ads.currency,
        tkqc.company,
        tkqc.ben_thue,
        tkqc.phi_thue,
        tkqc.dau_the,
        tkqc.company
),

ads_ladipageFacebook_total_with_tkqc AS (
    SELECT
        ads.*,
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY ads.date_start, ads.ma_nhan_vien, ads.ma_quan_ly, ads.brand, ads.channel
                ORDER BY ladi.date_insert
            ) = 1 THEN ladi.doanhThuLadi
            ELSE 0
        END AS doanhThuLadi
    FROM ads_total_with_tkqc AS ads
    LEFT JOIN {{ ref('t2_ladipage_facebook_total') }} AS ladi
        ON ads.date_start = ladi.date_insert
        AND ads.ma_nhan_vien = ladi.id_staff
        AND ads.ma_quan_ly = ladi.ma_quan_ly
        AND ads.brand = ladi.brand
        AND ads.channel = ladi.channel
        and ads.company = ladi.company
)

SELECT
    ads.date_start,
    ads.currency,
    ads.idtkqc,
    ads.nametkqc,
    ads.ma_nhan_vien,
    ads.staff,
    ads.ma_quan_ly,
    ads.manager,
    ads.brand,
    ads.channel,
    ads.chiPhiAds,
    case
    when ads.brand = 'Max Eagle' and ads.channel = 'Facebook'
    then 0
    else ads.doanhThuAds
    end as doanhThuAds,
    ads.doanhThuLadi,
    ads.revenue_type AS loaiDoanhThu,
    ads.company,
    ads.ben_thue,
    ads.phi_thue,
    ads.dau_the,
    ads.chi_phi_agency,
    CASE 
        WHEN lower(ads.ben_thue) = "build" THEN ads.chiPhiAds
        ELSE 0
    END AS ca_nhan
FROM ads_ladipageFacebook_total_with_tkqc AS ads -- where ads.company = 'One5'


----------------------------------------------------------------

-- WITH ads_total_with_tkqc AS (
-- SELECT
-- ads.date_start,
-- ads.revenue_type,
-- tkqc.idtkqc,
-- tkqc.nametkqc,
-- tkqc.ma_nhan_vien,
-- tkqc.staff,
-- tkqc.manager,
-- tkqc.ma_quan_ly,
-- tkqc.brand,
-- tkqc.channel,
-- ads.currency,
-- tkqc.company,
-- tkqc.ben_thue,
-- Max(tkqc.phi_thue) as phi_thue,
-- SUM(ads.spend) AS chiPhiAds,
-- SUM(ads.doanhThuAds) AS doanhThuAds,
-- SUM(ads.spend)* (1+ COALESCE(Max(tkqc.phi_thue),0)) as chi_phi_agency
-- FROM {{ ref('t2_ads_total')}} AS ads
-- RIGHT JOIN {{ ref('t2_tkqc_total') }} AS tkqc
-- ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
-- AND DATE(ads.date_start) >= DATE(tkqc.start_date)
-- AND (tkqc.end_date IS NULL OR DATE(ads.date_start) <= DATE(tkqc.end_date))
-- GROUP BY
-- ads.date_start,
-- tkqc.idtkqc,
-- tkqc.nametkqc,
-- tkqc.ma_nhan_vien,
-- tkqc.staff,
-- tkqc.manager,
-- tkqc.ma_quan_ly,
-- tkqc.brand,
-- tkqc.channel,
-- ads.revenue_type,
-- ads.currency,
-- tkqc.company,
-- tkqc.ben_thue
-- ),

-- ads_ladipageFacebook_total_with_tkqc AS (
-- SELECT
-- ads.*,
-- CASE
-- WHEN ROW_NUMBER() OVER (
-- PARTITION BY ads.date_start, ads.ma_nhan_vien, ads.ma_quan_ly, ads.brand, ads.channel
-- ORDER BY ladi.date_insert
-- ) = 1 THEN ladi.doanhThuLadi
-- ELSE 0
-- END AS doanhThuLadi,
-- FROM ads_total_with_tkqc AS ads
-- LEFT JOIN {{ ref('t2_ladipage_facebook_total') }} AS ladi
-- ON ads.date_start = ladi.date_insert
-- AND ads.ma_nhan_vien = ladi.id_staff
-- AND ads.ma_quan_ly = ladi.ma_quan_ly
-- AND ads.brand = ladi.brand
-- AND ads.channel = ladi.channel
-- )

-- -- ads_organic_total_with_tkqc AS (
-- -- SELECT
-- -- ads.*,
-- -- CASE
-- -- WHEN ROW_NUMBER() OVER (
-- -- PARTITION BY ads.date_start, ads.ma_nhan_vien, ads.ma_quan_ly, ads.brand, ads.channel
-- -- ORDER BY org.date_start
-- -- ) = 1 THEN org.doanhThuOrganic
-- -- ELSE 0
-- -- END AS doanhThuOrganic,
-- -- FROM ads_ladipageFacebook_total_with_tkqc AS ads
-- -- LEFT JOIN {{ref("t2_organic_total")}} AS org
-- -- ON ads.date_start = org.date_start
-- -- AND ads.ma_nhan_vien = org.ma_nhan_vien
-- -- AND ads.ma_quan_ly = org.ma_quan_ly
-- -- AND ads.brand = org.brand
-- -- AND ads.channel = org.channel
-- -- )

-- SELECT
-- ads.date_start,
-- ads.currency,
-- ads.idtkqc,
-- ads.nametkqc,
-- ads.ma_nhan_vien,
-- ads.staff,
-- ads.ma_quan_ly,
-- ads.manager,
-- ads.brand,
-- ads.channel,
-- ads.chiPhiAds,
-- ads.doanhThuAds,
-- ads.doanhThuLadi,
-- -- ads.doanhThuOrganic,
-- ads.revenue_type AS loaiDoanhThu,
-- ads.company,
-- ads.ben_thue,
-- ads.phi_thue,
-- ads.chi_phi_agency
-- FROM ads_ladipageFacebook_total_with_tkqc AS ads;

