WITH ads_daily AS (
  SELECT
    DATE(date_start) AS date_start,
    brand,
    channel,
    company,
    SUM(COALESCE(chiPhiAds, 0))                 AS chi_phi_ads,
    SUM(COALESCE(doanhThuAds, 0))               AS doanhThuAds,
    SUM(COALESCE(doanhThuLadi, 0))              AS doanhThuLadi,
    SUM(COALESCE(doanhThuAds, 0) + COALESCE(doanhThuLadi, 0)) AS doanh_thu_trinh_ads
  FROM {{ ref('t3_me_ads_total_with_tkqc') }}
  GROUP BY date_start, brand, channel,company
  ORDER BY 1,2,3,4
),

revenue_toa AS (
  SELECT
    DATE(ngay_tao_don)                           AS ngay_tao_don,         -- ngày chốt đơn
    brand,
    channel,
    company,
    SUM(COALESCE(doanh_thu_ke_toan, 0))         AS doanh_thu_ke_toan,
    SUM(COALESCE(gia_ban_daily_total, 0))       AS gia_ban_daily_total,
    SUM(COALESCE(tien_chiet_khau_sp, 0))        AS tien_chiet_khau_sp,
    SUM(COALESCE(tien_khach_hang_thanh_toan,0)) AS tien_khach_hang_thanh_toan
  FROM {{ ref('t3_me_revenue_all_channel') }}
  WHERE company = 'Max Eagle'
    AND ngay_tao_don IS NOT NULL
  GROUP BY 1,2,3,4
)

SELECT
  COALESCE(a.date_start, Cast(r.ngay_tao_don as date)) AS date_start,
  COALESCE(a.brand, r.brand) AS brand,
  COALESCE(a.channel, r.channel) AS channel,
  COALESCE(a.company, r.company) AS company,

-- Ads
  COALESCE(a.chi_phi_ads, 0)                    AS chi_phi_ads,
  COALESCE(a.doanh_thu_trinh_ads, 0)            AS doanh_thu_trinh_ads,
  COALESCE(a.doanhThuAds, 0)                    AS doanhThuAds,
  COALESCE(a.doanhThuLadi, 0)                   AS doanhThuLadi,

  -- Revenue
  COALESCE(r.doanh_thu_ke_toan, 0)              AS doanh_thu_ke_toan,
  COALESCE(r.gia_ban_daily_total, 0)            AS gia_ban_daily_total,
  COALESCE(r.tien_chiet_khau_sp, 0)             AS tien_chiet_khau_sp,
  COALESCE(r.tien_khach_hang_thanh_toan, 0)     AS tien_khach_hang_thanh_toan

FROM revenue_toa r
FULL OUTER JOIN ads_daily a
  ON CAST(r.ngay_tao_don as date) = a.date_start
  AND r.brand = a.brand
  AND r.channel = a.channel
  AND r.company = a.company
ORDER BY date_start DESC, brand, channel