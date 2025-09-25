WITH ads_daily AS (
  SELECT
    DATE(date_start) AS date_start,
    TRIM(brand_lv1) as brand_lv1,
    TRIM(brand) AS brand,
    CONCAT(UPPER(SUBSTR(channel, 1, 1)), LOWER(SUBSTR(channel, 2))) AS channel,
    company,
    TRIM(manager)                               AS manager,
    
    SUM(COALESCE(chiPhiAds, 0))                 AS chi_phi_ads,
    SUM(COALESCE(doanhThuAds, 0))               AS doanhThuAds,
    SUM(COALESCE(doanhThuLadi, 0))              AS doanhThuLadi,
    SUM(COALESCE(doanh_so_moi, 0))              AS doanh_so_moi,
    SUM(COALESCE(doanh_so_cu, 0))              AS doanh_so_cu,
    SUM(COALESCE(doanhThuAds, 0) + COALESCE(doanhThuLadi, 0)) AS doanh_thu_trinh_ads
  FROM {{ref("t3_me_ads_total_with_tkqc")}}
  WHERE company = 'Max Eagle' AND date_start IS NOT NULL
  GROUP BY date_start, brand, brand_lv1, channel,company, manager
),

revenue_toa AS (
  SELECT
    DATE(ngay_tao_don)                           AS ngay_tao_don,         -- ngày chốt đơn
    TRIM(brand) AS brand,
    TRIM(brand_lv1) AS brand_lv1,
    CONCAT(UPPER(SUBSTR(channel, 1, 1)), LOWER(SUBSTR(channel, 2))) AS channel,
    company,
    TRIM(manager)                               AS manager,
    -- TRIM(shop)                                  AS shop,
    SUM(COALESCE(doanh_so, 0))         AS doanh_so,
    SUM(COALESCE(doanh_thu_ke_toan, 0))         AS doanh_thu_ke_toan,
    SUM(COALESCE(gia_ban_daily_total, 0))       AS gia_ban_daily_total,
    SUM(COALESCE(tien_chiet_khau_sp, 0))        AS tien_chiet_khau_sp,
    SUM(COALESCE(tien_khach_hang_thanh_toan,0)) AS tien_khach_hang_thanh_toan
  FROM {{ref("t3_me_revenue_all_channel")}}
  WHERE company = 'Max Eagle' AND ngay_tao_don IS NOT NULL
  GROUP BY ngay_tao_don, brand, brand_lv1, channel, company, manager--, shop
)

SELECT
  COALESCE(a.date_start, Cast(r.ngay_tao_don as date)) AS date_start,
  COALESCE(a.brand, r.brand) AS brand,
  COALESCE(a.brand_lv1, r.brand_lv1) AS brand_lv1,
  COALESCE(a.channel, r.channel) AS channel,
  COALESCE(a.company, r.company) AS company,
  COALESCE(a.manager, r.manager) AS manager,
  

-- Ads
  COALESCE(a.chi_phi_ads, 0)                    AS chi_phi_ads,
  COALESCE(a.doanh_thu_trinh_ads, 0)            AS doanh_thu_trinh_ads,
  COALESCE(a.doanhThuAds, 0)                    AS doanhThuAds,
  COALESCE(a.doanhThuLadi, 0)                   AS doanhThuLadi,
  COALESCE(a.doanh_so_moi, 0)                  AS doanh_so_moi,
  COALESCE(a.doanh_so_cu, 0)                  AS doanh_so_cu,

  -- Revenue
  COALESCE(doanh_so, 0)         AS doanh_so,
  COALESCE(r.doanh_thu_ke_toan, 0)              AS doanh_thu_ke_toan,
  COALESCE(r.gia_ban_daily_total, 0)            AS gia_ban_daily_total,
  COALESCE(r.tien_chiet_khau_sp, 0)             AS tien_chiet_khau_sp,
  COALESCE(r.tien_khach_hang_thanh_toan, 0)     AS tien_khach_hang_thanh_toan

FROM revenue_toa r
FULL OUTER JOIN ads_daily a
  ON CAST(r.ngay_tao_don as date) = a.date_start
  AND r.brand = a.brand
  AND r.brand_lv1 = a.brand_lv1
  AND r.channel = a.channel
  AND r.company = a.company
  AND r.manager = a.manager
ORDER BY date_start DESC, brand, brand_lv1, channel