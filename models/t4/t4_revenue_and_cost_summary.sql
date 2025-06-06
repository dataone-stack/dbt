WITH revenue_daily AS (
  SELECT 
    DATE(ngay_tao_don) AS date_start,
    brand,
    channel,
    SUM(doanh_thu_ke_toan) AS doanh_thu_ke_toan
  FROM {{ ref('t3_revenue_all_channel') }}
  WHERE status NOT IN  ('Đã hủy')
  GROUP BY DATE(ngay_tao_don), brand, channel
),

ads_daily AS (
  SELECT
    date_start,
    brand,
    channel,
    SUM(chiPhiAds) AS chi_phi_ads,
    SUM(doanhThuAds) + SUM(doanhThuLadi) as doanh_thu_trinh_ads,
  FROM {{ ref('t3_ads_total_with_tkqc') }}
  WHERE chiPhiAds IS NOT NULL
  GROUP BY date_start, brand, channel
),

cir_max_monthly AS (
  SELECT
    year,
    month,
    brand,
    channel,
    AVG(CAST(cir_max AS FLOAT64)) AS avg_cir_max  -- Lấy trung bình cir_max
  FROM {{ ref('t1_cir_max') }}
  GROUP BY year, month, brand, channel
)

SELECT
  COALESCE(r.date_start, a.date_start) AS date_start,
  COALESCE(r.brand, a.brand) AS brand,
  COALESCE(r.channel, a.channel) AS channel,
  COALESCE(r.doanh_thu_ke_toan, 0) AS doanh_thu_ke_toan,
  COALESCE(a.chi_phi_ads, 0) AS chi_phi_ads,
  COALESCE(a.doanh_thu_trinh_ads, 0) AS doanh_thu_trinh_ads,
  SAFE_DIVIDE(COALESCE(a.chi_phi_ads, 0), COALESCE(r.doanh_thu_ke_toan, 0)) AS cir,
  EXTRACT(YEAR FROM COALESCE(r.date_start, a.date_start)) AS year,
  EXTRACT(MONTH FROM COALESCE(r.date_start, a.date_start)) AS month,
  cir_max.avg_cir_max AS cir_max,

FROM revenue_daily r
FULL OUTER JOIN ads_daily a
  ON r.date_start = a.date_start
  AND r.brand = a.brand
  AND r.channel = a.channel
LEFT JOIN cir_max_monthly AS cir_max
  ON EXTRACT(YEAR FROM COALESCE(r.date_start, a.date_start)) = CAST(cir_max.year AS INT64)
  AND EXTRACT(MONTH FROM COALESCE(r.date_start, a.date_start)) = CAST(cir_max.month AS INT64)
  AND COALESCE(r.brand, a.brand) = cir_max.brand
  AND COALESCE(r.channel, a.channel) = cir_max.channel
ORDER BY date_start DESC, brand, channel
