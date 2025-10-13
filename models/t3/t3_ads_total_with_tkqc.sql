
-- WITH ads_total_with_tkqc AS (
--     SELECT
--         ads.date_start,
--         ads.revenue_type,
--         -- Ưu tiên thông tin từ campaign mapping, fallback về tkqc
--         COALESCE(campaign_team.staff_code, tkqc.ma_nhan_vien) as ma_nhan_vien,
--         COALESCE(campaign_team.staff, tkqc.staff) as staff,
--         COALESCE(campaign_team.manager_code, tkqc.ma_quan_ly) as ma_quan_ly,
--         COALESCE(campaign_team.manager, tkqc.manager) as manager,

--         tkqc.idtkqc,
--         tkqc.nametkqc,
--         COALESCE(campaign_team.brand, tkqc.brand) as brand,
--         --Brand cho báo cáo ME
--         CASE 
--             WHEN COALESCE(campaign_team.brand, tkqc.brand) IN ('Cà Phê Mâm Xôi','MEG','NATURAL HEALTH')  THEN 'Mâm Xôi'
--             ELSE COALESCE(campaign_team.brand, tkqc.brand)
--         END AS brand_lv1,
--         tkqc.channel,
--         ads.currency,
--         tkqc.company,
--         tkqc.ben_thue,
--         tkqc.dau_the,
--         MAX(tkqc.phi_thue) as phi_thue,
--         SUM(ads.spend) AS chiPhiAds,
--         case
--         when (date(ads.date_start) <= '2025-09-30' and tkqc.channel = 'Facebook') or tkqc.channel != 'Facebook'
--         then  ROUND(SUM(ads.doanhThuAds), 0)
--         else 0
--         end AS doanhThuAds,
--         SUM(ads.spend) * (1 + COALESCE(MAX(tkqc.phi_thue), 0)) as chi_phi_agency
--     FROM (
--         -- Loại bỏ trùng lặp trong t2_ads_total trước khi JOIN
--         SELECT DISTINCT
--             date_start,
--             revenue_type,
--             account_id,
--             ad_id,
--             campaign_id,
--             currency,
--             spend,
--             doanhThuAds
--         from {{ref("t2_ads_total")}}
--     ) AS ads
-- -- RIGHT JOIN với tkqc như cũ
--      right JOIN {{ref("t2_tkqc_total")}} AS tkqc
--         ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
--         AND DATE(ads.date_start) >= DATE(tkqc.start_date)
--         AND (tkqc.end_date IS NULL OR DATE(ads.date_start) <= DATE(tkqc.end_date))
    
--     --LEFT JOIN với campaign team mapping trước
--     left  JOIN {{ref("t1_ads_campaign_by_team")}} AS campaign_team
--     ON CAST(ads.campaign_id AS STRING) = CAST(campaign_team.campaign_id AS STRING) and ads.account_id = campaign_team.account_id AND tkqc.end_date <= DATE('2025-08-31')
    
    
   
--     GROUP BY
--         ads.date_start,
--         tkqc.idtkqc,
--         tkqc.nametkqc,
--         COALESCE(campaign_team.staff_code, tkqc.ma_nhan_vien),
--         COALESCE(campaign_team.staff, tkqc.staff),
--         COALESCE(campaign_team.manager, tkqc.manager),
--         COALESCE(campaign_team.manager_code, tkqc.ma_quan_ly),
--         COALESCE(campaign_team.brand, tkqc.brand),
--         CASE 
--             WHEN COALESCE(campaign_team.brand, tkqc.brand) IN ('Cà Phê Mâm Xôi','MEG','NATURAL HEALTH')  THEN 'Mâm Xôi'
--             ELSE COALESCE(campaign_team.brand, tkqc.brand)
--         END,
--         tkqc.channel,
--         ads.revenue_type,
--         ads.currency,
--         tkqc.company,
--         tkqc.ben_thue,
--         tkqc.phi_thue,
--         tkqc.dau_the,
--         campaign_team.brand,
--         tkqc.brand
-- )


-- , ladipage_total AS (
--     SELECT 
--         company,manager_name,staff_name,date_insert,brand,channel,id_staff,ma_quan_ly,brand_lv1,
--         SUM(doanhThuLadi)    AS doanhThuLadi,
--         SUM(doanh_so_moi)    AS doanh_so_moi,
--         SUM(doanh_so_cu)     AS doanh_so_cu
--     FROM {{ref("t2_ladipage_facebook_total")}}
--     GROUP BY  date_insert,brand,brand_lv1,channel,id_staff,ma_quan_ly,company,staff_name,manager_name
-- )


-- ,ads_ladipageFacebook_total_with_tkqc AS (
--     SELECT
--         COALESCE(ads.date_start, ladi.date_insert) as date_start,
--         ladi.date_insert,
--         ads.revenue_type,
--         ads.idtkqc,
--         ads.nametkqc,
--         COALESCE(ads.ma_nhan_vien, ladi.id_staff) as ma_nhan_vien,
--         COALESCE(ads.staff, ladi.staff_name) as staff,
--         COALESCE(ads.manager, ladi.manager_name) as manager,
--         COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly) as ma_quan_ly,
--         COALESCE(ads.brand, ladi.brand) as brand,
--         --Brand cho báo cáo ME
--         CASE 
--             WHEN COALESCE(ads.brand, ladi.brand_lv1) IN ('Cà Phê Mâm Xôi','MEG','NATURAL HEALTH')  THEN 'Mâm Xôi'
--             ELSE COALESCE(ads.brand, ladi.brand_lv1)
--         END AS brand_lv1,

--         COALESCE(ads.channel, ladi.channel) as channel,
--         ads.currency,
--         COALESCE(ads.company, ladi.company) as company,
--         ads.ben_thue,
--         ads.dau_the,
--         ads.phi_thue,
--         COALESCE(ads.chiPhiAds, 0) as chiPhiAds,
--         COALESCE(ads.doanhThuAds, 0) as doanhThuAds,
--         COALESCE(ads.chi_phi_agency, 0) as chi_phi_agency,

--         CASE
--         WHEN ROW_NUMBER() OVER (
--             PARTITION BY 
--                 DATE(COALESCE(ads.date_start, ladi.date_insert)),
--                 COALESCE(ads.ma_nhan_vien, ladi.id_staff), 
--                 COALESCE(ads.staff, ladi.staff_name), 
--                 COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly), 
--                 COALESCE(ads.brand, ladi.brand),
--                 COALESCE(ads.brand_lv1, ladi.brand_lv1),
--                 COALESCE(ads.channel, ladi.channel),
--                 COALESCE(ads.company, ladi.company)
--             ORDER BY 
--                 CASE WHEN ladi.doanhThuLadi IS NOT NULL THEN 1 ELSE 2 END,
--                 ladi.date_insert,
--                 ads.date_start
--           ) = 1 THEN COALESCE(ladi.doanhThuLadi, 0)
--           ELSE 0
--         END AS doanhThuLadi,

--         CASE
--         WHEN ROW_NUMBER() OVER (
--             PARTITION BY 
--                 DATE(COALESCE(ads.date_start, ladi.date_insert)),
--                 COALESCE(ads.ma_nhan_vien, ladi.id_staff), 
--                 COALESCE(ads.staff, ladi.staff_name), 
--                 COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly), 
--                 COALESCE(ads.brand, ladi.brand),
--                 COALESCE(ads.brand_lv1, ladi.brand_lv1),
--                 COALESCE(ads.channel, ladi.channel),
--                 COALESCE(ads.company, ladi.company)
--             ORDER BY 
--                 CASE WHEN ladi.doanh_so_moi IS NOT NULL THEN 1 ELSE 2 END,
--                 ladi.date_insert,
--                 ads.date_start
--           ) = 1 THEN COALESCE(ladi.doanh_so_moi, 0)
--           ELSE 0
--         END AS doanh_so_moi,

--         CASE
--         WHEN ROW_NUMBER() OVER (
--             PARTITION BY 
--                 DATE(COALESCE(ads.date_start, ladi.date_insert)),
--                 COALESCE(ads.ma_nhan_vien, ladi.id_staff), 
--                 COALESCE(ads.staff, ladi.staff_name), 
--                 COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly), 
--                 COALESCE(ads.brand, ladi.brand),
--                 COALESCE(ads.brand_lv1, ladi.brand_lv1),
--                 COALESCE(ads.channel, ladi.channel),
--                 COALESCE(ads.company, ladi.company)
--             ORDER BY 
--                 CASE WHEN ladi.doanh_so_cu IS NOT NULL THEN 1 ELSE 2 END,
--                 ladi.date_insert,
--                 ads.date_start
--           ) = 1 THEN COALESCE(ladi.doanh_so_cu, 0)
--           ELSE 0
--         END AS doanh_so_cu

--     FROM ladipage_total AS ladi
--     FULL OUTER JOIN ads_total_with_tkqc AS ads
--         ON ladi.date_insert = ads.date_start
--         AND ads.ma_nhan_vien = ladi.id_staff
--         AND ads.ma_quan_ly = ladi.ma_quan_ly
--         AND ads.brand = ladi.brand
--         AND ads.brand_lv1 = ladi.brand_lv1
--         AND ads.channel = ladi.channel
--         AND ads.company = ladi.company
-- )
-- SELECT
--     ads.date_start,
--     ads.currency,
--     ads.idtkqc,
--     ads.nametkqc,
--     ads.ma_nhan_vien,
--     ads.staff,
--     ads.ma_quan_ly,
--     upper(trim(ads.manager)) as manager ,
--     ads.brand,
--     ads.brand_lv1,
--     ads.channel, 
--     ads.chiPhiAds,
--     ads.doanhThuAds,
--     ads.doanhThuLadi,
--     ads.doanh_so_moi,
--     ads.doanh_so_cu,

--     CASE 
--         WHEN ads.revenue_type = "" THEN "Organic"
--         WHEN ads.revenue_type is null THEN "Organic"
--         ELSE ads.revenue_type
--     END AS loaiDoanhThu,
--     ads.company,
--     ads.ben_thue,
--     ads.phi_thue,
--     ads.dau_the,
--     ads.chi_phi_agency,
--     CASE 
--         WHEN lower(ads.ben_thue) = "build" THEN ads.chiPhiAds
--         ELSE 0
--     END AS ca_nhan
-- FROM ads_ladipageFacebook_total_with_tkqc AS ads

WITH ads_total_with_tkqc AS (
    SELECT
        ads.date_start,
        ads.revenue_type,
        COALESCE(campaign_team.staff_code, tkqc.ma_nhan_vien) as ma_nhan_vien,
        COALESCE(campaign_team.staff, tkqc.staff) as staff,
        COALESCE(campaign_team.manager_code, tkqc.ma_quan_ly) as ma_quan_ly,
        COALESCE(campaign_team.manager, tkqc.manager) as manager,
        tkqc.idtkqc,
        tkqc.nametkqc,
        COALESCE(campaign_team.brand, tkqc.brand) as brand,
        CASE 
            WHEN COALESCE(campaign_team.brand, tkqc.brand) IN ('Cà Phê Mâm Xôi','MEG','NATURAL HEALTH') THEN 'Mâm Xôi'
            ELSE COALESCE(campaign_team.brand, tkqc.brand)
        END AS brand_lv1,
        tkqc.channel,
        ads.currency,
        tkqc.company,
        tkqc.ben_thue,
        tkqc.dau_the,
        MAX(tkqc.phi_thue) as phi_thue,
        SUM(ads.spend) AS chiPhiAds,
        CASE
            WHEN (DATE(ads.date_start) <= '2025-09-30' AND tkqc.channel = 'Facebook') OR tkqc.channel != 'Facebook'
            THEN ROUND(SUM(ads.doanhThuAds), 0)
            ELSE 0
        END AS doanhThuAds,
        SUM(ads.spend) * (1 + COALESCE(MAX(tkqc.phi_thue), 0)) as chi_phi_agency
    FROM (
        SELECT DISTINCT
            date_start,
            revenue_type,
            account_id,
            ad_id,
            campaign_id,
            currency,
            spend,
            doanhThuAds
        FROM `crypto-arcade-453509-i8`.`dtm`.`t2_ads_total`
    ) AS ads
    RIGHT JOIN `crypto-arcade-453509-i8`.`dtm`.`t2_tkqc_total` AS tkqc
        ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
        AND DATE(ads.date_start) >= DATE(tkqc.start_date)
        AND (tkqc.end_date IS NULL OR DATE(ads.date_start) <= DATE(tkqc.end_date))
    LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_ads_campaign_by_team` AS campaign_team
        ON CAST(ads.campaign_id AS STRING) = CAST(campaign_team.campaign_id AS STRING)
        AND ads.account_id = campaign_team.account_id
        AND tkqc.end_date <= DATE('2025-08-31')
    GROUP BY
        ads.date_start,
        tkqc.idtkqc,
        tkqc.nametkqc,
        COALESCE(campaign_team.staff_code, tkqc.ma_nhan_vien),
        COALESCE(campaign_team.staff, tkqc.staff),
        COALESCE(campaign_team.manager, tkqc.manager),
        COALESCE(campaign_team.manager_code, tkqc.ma_quan_ly),
        COALESCE(campaign_team.brand, tkqc.brand),
        CASE 
            WHEN COALESCE(campaign_team.brand, tkqc.brand) IN ('Cà Phê Mâm Xôi','MEG','NATURAL HEALTH') THEN 'Mâm Xôi'
            ELSE COALESCE(campaign_team.brand, tkqc.brand)
        END,
        tkqc.channel,
        ads.revenue_type,
        ads.currency,
        tkqc.company,
        tkqc.ben_thue,
        tkqc.phi_thue,
        tkqc.dau_the,
        campaign_team.brand,
        tkqc.brand
),

ladipage_total AS (
    SELECT 
        company,
        manager_name,
        staff_name,
        date_insert,
        brand,
        channel,
        id_staff,
        ma_quan_ly,
        brand_lv1,
        SUM(doanhThuLadi) AS doanhThuLadi,
        SUM(doanh_so_moi) AS doanh_so_moi,
        SUM(doanh_so_cu) AS doanh_so_cu
    FROM `crypto-arcade-453509-i8`.`dtm`.`t2_ladipage_facebook_total`
    GROUP BY date_insert, brand, brand_lv1, channel, id_staff, ma_quan_ly, company, staff_name, manager_name
),

ladi_unmatched AS (
    SELECT 
        ladi.date_insert AS date_start,
        'Facebook Ads' AS revenue_type,
        ladi.id_staff AS ma_nhan_vien,
        ladi.staff_name AS staff,
        ladi.ma_quan_ly AS ma_quan_ly,
        ladi.manager_name AS manager,
        ladi.brand_lv1 AS idtkqc,
        '' AS nametkqc,
        ladi.brand AS brand,
        ladi.brand_lv1 AS brand_lv1,
        ladi.channel AS channel,
        '' AS currency,
        ladi.company AS company,
        'build' AS ben_thue,
        NULL AS dau_the,
        NULL AS phi_thue,
        0 AS chiPhiAds,
        0 AS doanhThuAds,
        0 AS chi_phi_agency
    FROM ladipage_total AS ladi
    WHERE NOT EXISTS (
        SELECT 1
        FROM ads_total_with_tkqc AS ads
        WHERE ladi.date_insert = ads.date_start
          AND ads.ma_nhan_vien = ladi.id_staff
          AND ads.ma_quan_ly = ladi.ma_quan_ly
          AND ads.brand = ladi.brand
          AND ads.brand_lv1 = ladi.brand_lv1
          AND ads.channel = ladi.channel
          AND ads.company = ladi.company
    )
),

ads_extended AS (
    SELECT *
    FROM ads_total_with_tkqc
    UNION ALL
    SELECT *
    FROM ladi_unmatched
)

,ads_ladipageFacebook_total_with_tkqc AS (
    SELECT
        COALESCE(ads.date_start, ladi.date_insert) as date_start,
        ladi.date_insert,
        ads.revenue_type,
        ads.idtkqc,
        ads.nametkqc,
        COALESCE(ads.ma_nhan_vien, ladi.id_staff) as ma_nhan_vien,
        COALESCE(ads.staff, ladi.staff_name) as staff,
        COALESCE(ads.manager, ladi.manager_name) as manager,
        COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly) as ma_quan_ly,
        COALESCE(ads.brand, ladi.brand) as brand,
        --Brand cho báo cáo ME
        CASE 
            WHEN COALESCE(ads.brand, ladi.brand_lv1) IN ('Cà Phê Mâm Xôi','MEG','NATURAL HEALTH')  THEN 'Mâm Xôi'
            ELSE COALESCE(ads.brand, ladi.brand_lv1)
        END AS brand_lv1,

        COALESCE(ads.channel, ladi.channel) as channel,
        ads.currency,
        COALESCE(ads.company, ladi.company) as company,
        ads.ben_thue,
        ads.dau_the,
        ads.phi_thue,
        COALESCE(ads.chiPhiAds, 0) as chiPhiAds,
        COALESCE(ads.doanhThuAds, 0) as doanhThuAds,
        COALESCE(ads.chi_phi_agency, 0) as chi_phi_agency,

        CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY 
                DATE(COALESCE(ads.date_start, ladi.date_insert)),
                COALESCE(ads.ma_nhan_vien, ladi.id_staff), 
                COALESCE(ads.staff, ladi.staff_name), 
                COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly), 
                COALESCE(ads.brand, ladi.brand),
                COALESCE(ads.brand_lv1, ladi.brand_lv1),
                COALESCE(ads.channel, ladi.channel),
                COALESCE(ads.company, ladi.company)
            ORDER BY 
                CASE WHEN ladi.doanhThuLadi IS NOT NULL THEN 1 ELSE 2 END,
                ladi.date_insert,
                ads.date_start
          ) = 1 THEN COALESCE(ladi.doanhThuLadi, 0)
          ELSE 0
        END AS doanhThuLadi,
        CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY 
                DATE(COALESCE(ads.date_start, ladi.date_insert)),
                COALESCE(ads.ma_nhan_vien, ladi.id_staff), 
                COALESCE(ads.staff, ladi.staff_name), 
                COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly), 
                COALESCE(ads.brand, ladi.brand),
                COALESCE(ads.brand_lv1, ladi.brand_lv1),
                COALESCE(ads.channel, ladi.channel),
                COALESCE(ads.company, ladi.company)
            ORDER BY 
                CASE WHEN ladi.doanh_so_moi IS NOT NULL THEN 1 ELSE 2 END,
                ladi.date_insert,
                ads.date_start
          ) = 1 THEN COALESCE(ladi.doanh_so_moi, 0)
          ELSE 0
        END AS doanh_so_moi,

        CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY 
                DATE(COALESCE(ads.date_start, ladi.date_insert)),
                COALESCE(ads.ma_nhan_vien, ladi.id_staff), 
                COALESCE(ads.staff, ladi.staff_name), 
                COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly), 
                COALESCE(ads.brand, ladi.brand),
                COALESCE(ads.brand_lv1, ladi.brand_lv1),
                COALESCE(ads.channel, ladi.channel),
                COALESCE(ads.company, ladi.company)
            ORDER BY 
                CASE WHEN ladi.doanh_so_cu IS NOT NULL THEN 1 ELSE 2 END,
                ladi.date_insert,
                ads.date_start
          ) = 1 THEN COALESCE(ladi.doanh_so_cu, 0)
          ELSE 0
        END AS doanh_so_cu

    FROM ladipage_total AS ladi
    FULL OUTER JOIN ads_extended AS ads
        ON ladi.date_insert = ads.date_start
        AND ads.ma_nhan_vien = ladi.id_staff
        AND ads.ma_quan_ly = ladi.ma_quan_ly
        AND ads.brand = ladi.brand
        AND ads.brand_lv1 = ladi.brand_lv1
        AND ads.channel = ladi.channel
        AND ads.company = ladi.company
),

a as (
SELECT
    ads.date_start,
    ads.currency,
    ads.idtkqc,
    ads.nametkqc,
    ads.ma_nhan_vien,
    ads.staff,
    ads.ma_quan_ly,
    upper(trim(ads.manager)) as manager ,
    ads.brand,
    ads.brand_lv1,
    ads.channel, 
    ads.chiPhiAds,
    ads.doanhThuAds,
    ads.doanhThuLadi,
    ads.doanh_so_moi,
    ads.doanh_so_cu,

    CASE 
        WHEN ads.revenue_type = "" THEN "Organic"
        WHEN ads.revenue_type is null THEN "Organic"
        ELSE ads.revenue_type
    END AS loaiDoanhThu,
    ads.company,
    ads.ben_thue,
    ads.phi_thue,
    ads.dau_the,
    ads.chi_phi_agency,
    CASE 
        WHEN lower(ads.ben_thue) = "build" THEN ads.chiPhiAds
        ELSE 0
    END AS ca_nhan
FROM ads_ladipageFacebook_total_with_tkqc AS ads
)

select * from a

-- ----------------
-- WITH ads_total_with_tkqc AS (
--     SELECT
--         ads.date_start,
--         ads.revenue_type,
--         tkqc.idtkqc,
--         tkqc.nametkqc,
--         tkqc.ma_nhan_vien,
--         tkqc.staff,
--         tkqc.manager,
--         tkqc.ma_quan_ly,
--         tkqc.brand,
--         tkqc.channel,
--         ads.currency,
--         tkqc.company,
--         tkqc.ben_thue,
--         tkqc.dau_the,
--         MAX(tkqc.phi_thue) as phi_thue,
--         SUM(ads.spend) AS chiPhiAds,
--         SUM(ads.doanhThuAds) AS doanhThuAds,
--         SUM(ads.spend) * (1 + COALESCE(MAX(tkqc.phi_thue), 0)) as chi_phi_agency
--     FROM (
--         -- Loại bỏ trùng lặp trong t2_ads_total trước khi JOIN
--         SELECT DISTINCT
--             date_start,
--             revenue_type,
--             account_id,
--             ad_id,
--             campaign_id,
--             currency,
--             spend,
--             doanhThuAds
--         FROM `crypto-arcade-453509-i8`.`dtm`.`t2_ads_total`
--     ) AS ads
--     RIGHT JOIN `crypto-arcade-453509-i8`.`dtm`.`t2_tkqc_total` AS tkqc
--         ON CAST(ads.account_id AS STRING) = CAST(tkqc.idtkqc AS STRING)
--         AND DATE(ads.date_start) >= DATE(tkqc.start_date)
--         AND (tkqc.end_date IS NULL OR DATE(ads.date_start) <= DATE(tkqc.end_date))
--     GROUP BY
--         ads.date_start,
--         tkqc.idtkqc,
--         tkqc.nametkqc,
--         tkqc.ma_nhan_vien,
--         tkqc.staff,
--         tkqc.manager,
--         tkqc.ma_quan_ly,
--         tkqc.brand,
--         tkqc.channel,
--         ads.revenue_type,
--         ads.currency,
--         tkqc.company,
--         tkqc.ben_thue,
--         tkqc.phi_thue,
        
--         tkqc.dau_the
-- ),

-- ads_ladipageFacebook_total_with_tkqc AS (
--     SELECT
--         COALESCE(ads.date_start, ladi.date_insert) as date_start,
--         ladi.date_insert,
--         ads.revenue_type,
--         ads.idtkqc,
--         ads.nametkqc,
--         COALESCE(ads.ma_nhan_vien, ladi.id_staff) as ma_nhan_vien,
--         COALESCE(ads.staff, ladi.staff_name) as staff,
--         COALESCE(ads.manager, ladi.manager_name) as manager,
--         COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly) as ma_quan_ly,
--         COALESCE(ads.brand, ladi.brand) as brand,
--         COALESCE(ads.channel, ladi.channel) as channel,
--         ads.currency,
--         COALESCE(ads.company, ladi.company) as company,
--         ads.ben_thue,
--         ads.dau_the,
--         ads.phi_thue,
--         COALESCE(ads.chiPhiAds, 0) as chiPhiAds,
--         COALESCE(ads.doanhThuAds, 0) as doanhThuAds,
--         COALESCE(ads.chi_phi_agency, 0) as chi_phi_agency,
--         CASE
--             WHEN ROW_NUMBER() OVER (
--                 PARTITION BY COALESCE(ads.date_start, ladi.date_insert), 
--                            COALESCE(ads.ma_nhan_vien, ladi.id_staff), 
--                            COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly), 
--                            COALESCE(ads.brand, ladi.brand), 
--                            COALESCE(ads.channel, ladi.channel)
--                 ORDER BY COALESCE(ladi.date_insert, ads.date_start)
--             ) = 1 THEN COALESCE(ladi.doanhThuLadi, 0)
--             ELSE 0
--         END AS doanhThuLadi,
--         -- COALESCE(ladi.doanhThuLadi, 0) as doanh_thu_ladi_new
--     FROM `crypto-arcade-453509-i8`.`dtm`.`t2_ladipage_facebook_total` AS ladi
--     FULL OUTER JOIN ads_total_with_tkqc AS ads
--         ON ladi.date_insert = ads.date_start
--         AND ads.ma_nhan_vien = ladi.id_staff
--         AND ads.ma_quan_ly = ladi.ma_quan_ly
--         AND ads.brand = ladi.brand
--         AND ads.channel = ladi.channel
--         AND ads.company = ladi.company
-- )

-- SELECT
--     ads.date_start,
--     ads.currency,
--     ads.idtkqc,
--     ads.nametkqc,
--     ads.ma_nhan_vien,
--     ads.staff,
--     ads.ma_quan_ly,
--     ads.manager,
--     ads.brand,
--     ads.channel,
--     ads.chiPhiAds,
--     ads.doanhThuAds,
--     ads.doanhThuLadi,
--     -- ads.doanh_thu_ladi_new,
--     CASE 
--         WHEN ads.revenue_type = "" THEN "Organic"
--         WHEN ads.revenue_type is null THEN "Organic"
--         ELSE ads.revenue_type
--     END AS loaiDoanhThu,
--     ads.company,
--     ads.ben_thue,
--     ads.phi_thue,
--     ads.dau_the,
--     ads.chi_phi_agency,
--     CASE 
--         WHEN lower(ads.ben_thue) = "build" THEN ads.chiPhiAds
--         ELSE 0
--     END AS ca_nhan
-- FROM ads_ladipageFacebook_total_with_tkqc AS ads