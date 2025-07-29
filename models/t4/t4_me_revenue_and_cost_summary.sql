--CTE ads_daily tổng hợp dữ liệu quảng cáo
WITH ads_daily AS (
  SELECT
    date_start,
    brand,
    channel,
    company,
    SUM(COALESCE(chiPhiAds, 0)) AS chi_phi_ads, -- Tổng chi phí quảng cáo, thay NULL bằng 0
    SUM(COALESCE(doanhThuAds, 0)) + SUM(COALESCE(doanhThuLadi, 0)) AS doanh_thu_trinh_ads, -- Tổng doanh thu từ trình quảng cáo: doanh thu Ads + doanh thu từ Ladi
    SUM(COALESCE(doanhThuAds, 0)) AS doanhThuAds,
    SUM(COALESCE(doanhThuLadi, 0)) AS doanhThuLadi,
  FROM {{ ref("t3_me_ads_total_with_tkqc") }}
  WHERE chiPhiAds IS NOT NULL
  GROUP BY date_start, brand, channel,company
),
--CTE cir_max_monthly tính toán trung bình chỉ số cir_max
cir_max_monthly AS (
  SELECT
    year,
    month,
    brand,
    channel,
    AVG(CAST(cir_max AS FLOAT64)) AS avg_cir_max,
    AVG(CAST(cir_max_ads AS FLOAT64)) AS avg_cir_max_ads  -- Lấy trung bình cir_max
  FROM {{ ref('t1_cir_max') }}
  GROUP BY year, month, brand, channel
),
cir_max_ads_monthly AS (
  SELECT
    year,
    month,
    brand,
    channel,
    AVG(CAST(cir_max AS FLOAT64)) AS avg_cir_max  -- Lấy trung bình cir_max
  FROM {{ ref('t1_cir_max_ads') }}
  GROUP BY year, month, brand, channel
),
-- CTE revenue_tot tổng hợp doanh thu
revenue_tot AS (
  SELECT DISTINCT
    brand, 
    company,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(date_create)) as date_start, 
-- Loại bỏ các đơn hàng có tổng_amount nhỏ hơn 60,000
    case
        when SUM(total_amount) < 60000
        then 0
        else SUM(total_amount)
    end as total_amount,
    case
        when SUM(total_amount) < 60000
        then 0
        else  SUM(gia_ban_daily_total)
    end as gia_ban_daily_total,
    case
        when SUM(total_amount) < 60000
        then 0
        else  SUM(doanh_thu_ke_toan)
    end as doanh_thu_ke_toan,
    channel,
    case
        when SUM(total_amount) < 60000
        then 0
        else SUM(tien_chiet_khau_sp_tot) 
    end as tien_chiet_khau_sp_tot,

    SUM(phu_phi) as phu_phi
  FROM {{ ref("t3_me_revenue_all_channel_tot") }}
  WHERE date_create IS NOT NULL
  GROUP BY date_start, brand, channel, company
)
SELECT
  COALESCE(a.date_start, Cast(r_tot.date_start as date)) AS date_start,
  COALESCE(a.brand, r_tot.brand) AS brand,
  COALESCE(a.channel, r_tot.channel) AS channel,
  COALESCE(a.company, r_tot.company) AS company,
  COALESCE(a.chi_phi_ads, 0) AS chi_phi_ads,
  COALESCE(a.doanh_thu_trinh_ads, 0) AS doanh_thu_trinh_ads,
  COALESCE(a.doanhThuAds, 0) AS doanhThuAds,
  COALESCE(a.doanhThuLadi, 0) AS doanhThuLadi,
  EXTRACT(YEAR FROM COALESCE(a.date_start, Cast(r_tot.date_start as date))) AS year,
  EXTRACT(MONTH FROM COALESCE(a.date_start, Cast(r_tot.date_start as date))) AS month,
  cir_max.avg_cir_max AS cir_max,
  cir_max.avg_cir_max_ads AS cir_max_ads,
  r_tot.total_amount as total_amount_paid_tot,
  r_tot.gia_ban_daily_total as gia_ban_daily_total_tot,
  r_tot.doanh_thu_ke_toan as doanh_thu_ke_toan_tot,
  r_tot.tien_chiet_khau_sp_tot,
  r_tot.phu_phi,

FROM revenue_tot r_tot
FULL OUTER JOIN ads_daily a
  ON Cast(r_tot.date_start as date) = a.date_start
  AND r_tot.brand = a.brand
  AND r_tot.channel = a.channel
  AND r_tot.company = a.company
LEFT JOIN cir_max_monthly AS cir_max
  ON EXTRACT(YEAR FROM COALESCE( a.date_start, Cast(r_tot.date_start as date))) = CAST(cir_max.year AS INT64)
  AND EXTRACT(MONTH FROM COALESCE( a.date_start, Cast(r_tot.date_start as date))) = CAST(cir_max.month AS INT64)
  AND COALESCE( a.brand, r_tot.brand) = cir_max.brand
  AND COALESCE( a.channel, r_tot.brand) = cir_max.channel

ORDER BY date_start DESC, brand, channel


