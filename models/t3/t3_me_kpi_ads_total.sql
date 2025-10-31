WITH ads_total AS (
  SELECT 
    brand_lv1 AS brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    company,
    COALESCE(SUM(doanhThuads + doanh_so_moi), 0) AS DoanhThuAds,
    COALESCE(SUM(chiPhiAds),0) AS chiPhiAds,
    COALESCE(ROUND(SAFE_DIVIDE(SUM(chiPhiAds), SUM(doanhThuads + doanh_so_moi)), 4),0) AS cir,

  FROM {{ref('t3_ads_total_with_tkqc')}}
  WHERE company = 'Max Eagle' 
  GROUP BY
    brand_lv1,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    company
)
SELECT 
  a.brand,
  a.channel,
  a.date_start,
  a.ma_nhan_vien,
  a.staff,
  a.ma_quan_ly,
  a.manager,
  a.company,
  SUM(a.DoanhThuAds) AS DoanhThuAds,
  SUM(a.chiPhiAds) AS chiPhiAds,
  MAX(a.cir) AS cir,

  COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)) AS kpi_month,
  COALESCE(b.year, EXTRACT(YEAR FROM a.date_start)) AS kpi_year,
  COALESCE(SUM(b.revenue_target),0) AS revenue_target,
  COALESCE(SUM(b.spend),0) AS spend,
  COALESCE(MAX(b.cir_target),0) AS cir_target
FROM ads_total a
FULL OUTER JOIN {{ref('t1_kpi_ads_total')}} b
  ON TRIM(a.brand) = TRIM(b.brand)
  AND TRIM(a.channel) = TRIM(b.channel)
  AND TRIM(a.ma_nhan_vien) = TRIM(b.ma_nhan_vien)
  AND TRIM(a.ma_quan_ly) = TRIM(b.manager_code)
  AND TRIM(a.company) = TRIM(b.company)
  AND EXTRACT(MONTH FROM a.date_start) = b.month
  AND EXTRACT(YEAR FROM a.date_start) = b.year
  GROUP BY
    brand,
    channel,
    date_start,
    ma_nhan_vien,
    staff,
    ma_quan_ly,
    manager,
    company,
    COALESCE(b.month, EXTRACT(MONTH FROM a.date_start)),
    COALESCE(b.year, EXTRACT(YEAR FROM a.date_start))