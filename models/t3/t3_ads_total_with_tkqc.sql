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
        ads.account_name as nametkqc_trinh,
        COALESCE(campaign_team.brand, tkqc.brand) as brand,
        COALESCE(campaign_team.brand, tkqc.brand) AS brand_lv1,
        tkqc.channel,
        ads.currency,
        tkqc.company,
        -- tkqc.company_lv1,
        tkqc.ben_thue,
        tkqc.dau_the,
        MAX(tkqc.phi_thue) as phi_thue,
        tkqc.so_tai_khoan,
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
            account_name,
            ad_id,
            campaign_id,
            campaign_name,
            currency,
            spend,
            doanhThuAds
        FROM `crypto-arcade-453509-i8`.`dtm`.`t2_ads_total`
        WHERE 
            (
                account_id = '2157355114664606'
                AND date(date_start) <= '2025-10-31'
            )
            OR account_id <> '2157355114664606'
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
        ads.account_name,
        COALESCE(campaign_team.staff_code, tkqc.ma_nhan_vien),
        COALESCE(campaign_team.staff, tkqc.staff),
        COALESCE(campaign_team.manager, tkqc.manager),
        COALESCE(campaign_team.manager_code, tkqc.ma_quan_ly),
        COALESCE(campaign_team.brand, tkqc.brand),
        COALESCE(campaign_team.brand, tkqc.brand),
        tkqc.channel,
        ads.revenue_type,
        ads.currency,
        tkqc.company,
        tkqc.ben_thue,
        tkqc.phi_thue,
        -- tkqc.company_lv1,
        tkqc.dau_the,
        campaign_team.brand,
        tkqc.brand,
        tkqc.so_tai_khoan
)

,case_tkqc_chay_nhieu_nguoi as (
select 
  a.date as date_start,
  'Facebook Ads' as revenue_type,
  b.ma_nhan_vien,
  b.staff,
  b.ma_quan_ly,
  b.manager,
  cast(a.idtkqc as string) as idtkqc,
  b.nametkqc,
  b.nametkqc as nametkqc_trinh,
 
  b.brand,
  b.brand as brand_lv1,
  b.channel,

  'VND' as currency,
  b.company,
    
  b.ben_thue,
  b.dau_the,

  MAX(b.phi_thue) as phi_thue,
  b.so_tai_khoan,
  SUM(a.chi_phi) AS chiPhiAds,
  0 AS doanhThuAds,
  SUM(a.chi_phi) * (1 + COALESCE(MAX(b.phi_thue), 0)) as chi_phi_agency

from `dtm.t1_case_tkqc_nhieu_nguoi_chay` a
left join `dtm.t2_tkqc_total` b on cast(a.idtkqc as string) = b.idtkqc and a.ma_nv = b.ma_nhan_vien and a.date between b.start_date and b.end_date

GROUP BY
  a.date,
  b.ma_nhan_vien,
  b.staff,
  b.ma_quan_ly,
  b.manager,    
  a.idtkqc,
  b.nametkqc,
  b.brand,
  b.channel,
  b.company,
  b.ben_thue,
  b.dau_the,
  b.so_tai_khoan
)

--select sum(chiPhiAds) from case_tkqc_chay_nhieu_nguoi where ma_nhan_vien = 'MEG1150'

, 

ads_total_with_tkqc_total as (
  select * from ads_total_with_tkqc
  union all
  select * from case_tkqc_chay_nhieu_nguoi
)

,
ladipage_total AS (
    SELECT 
        company,
        manager_name,
        case
        when staff_name = 'Admin Đơn Vị'
        then (SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2))) , ' ') FROM UNNEST(SPLIT(manager_name, ' ')) AS word)
        else staff_name
        end as staff_name,
        
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
        '' as nametkqc_trinh,
        ladi.brand AS brand,
        ladi.brand_lv1 AS brand_lv1,
        ladi.channel AS channel,
        '' AS currency,
        ladi.company AS company,
        -- one.company_lv1,
        'build' AS ben_thue,
        NULL AS dau_the,
        NULL AS phi_thue,
        '0' as so_tai_khoan,
        0 AS chiPhiAds,
        0 AS doanhThuAds,
        0 AS chi_phi_agency
    FROM ladipage_total AS ladi
    
    WHERE NOT EXISTS (
        SELECT 1
        FROM ads_total_with_tkqc_total AS ads
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
    FROM ads_total_with_tkqc_total
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
        ads.nametkqc_trinh,
        COALESCE(ads.ma_nhan_vien, ladi.id_staff) as ma_nhan_vien,
        COALESCE(ads.staff, ladi.staff_name) as staff,
        COALESCE(ads.manager, ladi.manager_name) as manager,
        COALESCE(ads.ma_quan_ly, ladi.ma_quan_ly) as ma_quan_ly,
        COALESCE(ads.brand, ladi.brand) as brand,
        --Brand cho báo cáo ME
        COALESCE(ads.brand, ladi.brand_lv1) AS brand_lv1,
        COALESCE(ads.channel, ladi.channel) as channel,
        ads.currency,
        COALESCE(ads.company, ladi.company) as company,
        -- ads.company_lv1,
        ads.ben_thue,
        ads.dau_the,
        ads.phi_thue,
        ads.so_tai_khoan,
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
        AND ads.staff = ladi.staff_name
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
    ads.nametkqc_trinh,
    ads.ma_nhan_vien,
    ads.staff,
    ads.ma_quan_ly,
    upper(trim(ads.manager)) as manager ,
    ads.brand,
    ads.brand_lv1,
    ads.channel, 
    ads.so_tai_khoan,
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
    -- ads.company_lv1,
    ads.ben_thue,
    ads.phi_thue,
    ads.dau_the,
    ads.chi_phi_agency,
    CASE 
        WHEN lower(ads.ben_thue) = "build" THEN ads.chiPhiAds
        ELSE 0
    END AS ca_nhan,
    0 as doanh_so_san,
    0 as tong_phi_san
FROM ads_ladipageFacebook_total_with_tkqc AS ads
)
,
shop as (
  select date(ngay_tao_don) as ngay_tao_don,shop_id,shop, brand,brand_lv1,channel, sum(doanh_so_san) as doanh_so_san, sum(tong_phi_san) as tong_phi_san from `crypto-arcade-453509-i8`.`dtm`.`t3_revenue_all_channel`  where channel in ('Tiktok','Shopee')
  group by date(ngay_tao_don),shop_id,brand,shop,channel,brand_lv1
)

, b as (select shop.ngay_tao_don as date_start, '-' as curency,shop.shop_id as idtkqc,shop.shop as nametkqc,shop.shop as nametkqc_trinh,tkqc.ma_nhan_vien,tkqc.staff,tkqc.ma_quan_ly,tkqc.manager,shop.brand,shop.brand_lv1,shop.channel,'0' as so_tai_khoan, 0 as chiPhiAds,0 as doanhThuAds, 0 as doanhThuLadi, 0 as doanh_so_moi, 0 as doanh_so_cu,'Shop seller' as loaiDoanhThu,tkqc.company, tkqc.ben_thue,tkqc.phi_thue,tkqc.dau_the,0 as chi_phi_agency,0 as ca_nhan, shop.doanh_so_san,shop.tong_phi_san from shop
left join `dtm.t2_tkqc_total` tkqc on shop.shop_id = tkqc.idtkqc and shop.ngay_tao_don between tkqc.start_date and tkqc.end_date )

select * from a 
union all
select * from b