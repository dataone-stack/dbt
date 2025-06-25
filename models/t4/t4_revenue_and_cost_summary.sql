WITH revenue_daily AS (
  SELECT 
    DATE(ngay_tao_don) AS date_start,
    brand,
    channel,
    -- ten_san_pham,
    -- sku_code,
    -- SUM(so_luong) as so_luong,
    -- SUM(tien_khach_hang_thanh_toan) AS tien_khach_hang_thanh_toan,
    -- SUM(tien_sp_sau_tro_gia) AS tien_sp_sau_tro_gia,
    -- SUM(phi_ship) AS phi_ship,
    -- SUM(giam_gia_seller) AS giam_gia_seller,
    -- SUM(giam_gia_san) AS giam_gia_san,
    -- SUM(seller_tro_gia) AS seller_tro_gia,
    -- SUM(san_tro_gia) AS san_tro_gia,
    -- SUM(tong_phi_san) AS tong_phi_san,

    -- SUM(phi_van_chuyen_thuc_te) AS phi_van_chuyen_thuc_te,
    -- SUM(phi_van_chuyen_tro_gia_tu_san) AS phi_van_chuyen_tro_gia_tu_san,
    -- SUM(phi_thanh_toan) AS phi_thanh_toan,
    -- SUM(phi_hoa_hong_shop) AS phi_hoa_hong_shop,
    -- SUM(phi_hoa_hong_tiep_thi_lien_ket) AS phi_hoa_hong_tiep_thi_lien_ket,
    -- SUM(phi_hoa_hong_quang_cao_cua_hang) AS phi_hoa_hong_quang_cao_cua_hang,
    -- SUM(phi_dich_vu) AS phi_dich_vu,
    -- SUM(phi_xtra) AS phi_xtra,
    -- SUM(voucher_from_seller) AS voucher_from_seller,
    -- SUM(phi_co_dinh) AS phi_co_dinh,

    SUM(doanh_thu_ke_toan) AS doanh_thu_ke_toan,
    SUM(tien_chiet_khau_sp ) AS tien_chiet_khau_sp,
    SUM(gia_san_pham_goc_total ) AS gia_san_pham_goc_total,
    SUM(gia_ban_daily_total ) AS gia_ban_daily_total,
  FROM {{ ref('t3_revenue_all_channel') }}
  WHERE status NOT IN  ('Đã hủy')
  GROUP BY DATE(ngay_tao_don), brand, channel --,ten_san_pham,sku_code
),

ads_daily AS (
  SELECT
    date_start,
    brand,
    channel,
    SUM(COALESCE(chiPhiAds, 0)) AS chi_phi_ads,
    SUM(COALESCE(doanhThuAds, 0)) + SUM(COALESCE(doanhThuLadi, 0)) AS doanh_thu_trinh_ads,
    SUM(COALESCE(doanhThuAds, 0)) AS doanhThuAds,
    SUM(COALESCE(doanhThuLadi, 0)) AS doanhThuLadi,
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

revenue_tot AS (
  SELECT
    brand, 
    date_create as date_start, 
    tong_tien_da_thanh_toan as total_amount, 
    channel
  FROM {{ ref('t3_revenue_all_channel_tot') }}
  WHERE date_create IS NOT NULL
  GROUP BY date_start, brand, channel
),
SELECT
  COALESCE(r.date_start, a.date_start) AS date_start,
  COALESCE(r.brand, a.brand) AS brand,
  COALESCE(r.channel, a.channel) AS channel,
--   COALESCE(r.so_luong) AS so_luong,
--   COALESCE(r.ten_san_pham) AS ten_san_pham,
--   COALESCE(r.sku_code) AS sku_code,
  
--   COALESCE(r.tien_khach_hang_thanh_toan, 0) AS tien_khach_hang_thanh_toan,
--   COALESCE(r.tien_sp_sau_tro_gia, 0) AS tien_sp_sau_tro_gia,
--   COALESCE(r.phi_ship, 0) AS phi_ship,
--   COALESCE(r.giam_gia_seller, 0) AS giam_gia_seller,
--   COALESCE(r.giam_gia_san, 0) AS giam_gia_san,
--   COALESCE(r.seller_tro_gia, 0) AS seller_tro_gia,
--   COALESCE(r.san_tro_gia, 0) AS san_tro_gia,
--   COALESCE(r.tong_phi_san, 0) AS tong_phi_san,
  
--   COALESCE(r.phi_van_chuyen_thuc_te, 0) AS phi_van_chuyen_thuc_te,
--     COALESCE(r.phi_van_chuyen_tro_gia_tu_san, 0) AS phi_van_chuyen_tro_gia_tu_san,
--     COALESCE(r.phi_thanh_toan, 0) AS phi_thanh_toan,
--     COALESCE(r.phi_hoa_hong_shop, 0) AS phi_hoa_hong_shop,
--     COALESCE(r.phi_hoa_hong_tiep_thi_lien_ket, 0) AS phi_hoa_hong_tiep_thi_lien_ket,
--     COALESCE(r.phi_hoa_hong_quang_cao_cua_hang, 0) AS phi_hoa_hong_quang_cao_cua_hang,
--     COALESCE(r.phi_dich_vu, 0) AS phi_dich_vu,
--     COALESCE(r.phi_xtra, 0) AS phi_xtra,
--     COALESCE(r.voucher_from_seller, 0) AS voucher_from_seller,
--     COALESCE(r.phi_co_dinh, 0) AS phi_co_dinh,

  COALESCE(r.doanh_thu_ke_toan, 0) AS doanh_thu_ke_toan,
  COALESCE(a.chi_phi_ads, 0) AS chi_phi_ads,
  COALESCE(a.doanh_thu_trinh_ads, 0) AS doanh_thu_trinh_ads,
  SAFE_DIVIDE(COALESCE(a.chi_phi_ads, 0), COALESCE(r.doanh_thu_ke_toan, 0)) AS cir,
  SAFE_DIVIDE(COALESCE(a.chi_phi_ads, 0), COALESCE(r.gia_ban_daily_total, 0)) AS cir_doanh_so,
  SAFE_DIVIDE(COALESCE(a.chi_phi_ads, 0), COALESCE(a.doanh_thu_trinh_ads, 0)) AS cir_trinh_ads,
  COALESCE(r.tien_chiet_khau_sp, 0) AS tien_chiet_khau_sp,
  COALESCE(r.gia_san_pham_goc_total, 0) AS gia_san_pham_goc_total,
  COALESCE(r.gia_ban_daily_total, 0) AS gia_ban_daily_total,
  COALESCE(a.doanhThuAds, 0) AS doanhThuAds,
  COALESCE(a.doanhThuLadi, 0) AS doanhThuLadi,
  EXTRACT(YEAR FROM COALESCE(r.date_start, a.date_start)) AS year,
  EXTRACT(MONTH FROM COALESCE(r.date_start, a.date_start)) AS month,
  cir_max.avg_cir_max AS cir_max,
  r_tot.total_amount as total_amount_paid

FROM revenue_daily r
FULL OUTER JOIN ads_daily a
  ON r.date_start = a.date_start
  AND r.brand = a.brand
  AND r.channel = a.channel
FULL OUTER JOIN revenue_tot r_tot
  ON r.date_start = r_tot.date_start
  AND r.brand = r_tot.brand
  AND r.channel = r_tot.channel
LEFT JOIN cir_max_monthly AS cir_max
  ON EXTRACT(YEAR FROM COALESCE(r.date_start, a.date_start)) = CAST(cir_max.year AS INT64)
  AND EXTRACT(MONTH FROM COALESCE(r.date_start, a.date_start)) = CAST(cir_max.month AS INT64)
  AND COALESCE(r.brand, a.brand) = cir_max.brand
  AND COALESCE(r.channel, a.channel) = cir_max.channel
ORDER BY date_start DESC, brand, channel
