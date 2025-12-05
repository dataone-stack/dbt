--CTE ads_daily tổng hợp dữ liệu quảng cáo
WITH ads_daily AS (
  SELECT
    date_start,
    TRIM(brand) as brand,
    -- brand_lv1,
    TRIM(CONCAT(UPPER(SUBSTR(tkqc.channel, 1, 1)), LOWER(SUBSTR(tkqc.channel, 2)))) AS channel,
    tkqc.company,
    manager,
    a.shopid as shop_id,
    a.shopname as shop,
    -- company_lv1,
    SUM(COALESCE(chiPhiAds, 0)) AS chi_phi_ads, -- Tổng chi phí quảng cáo, thay NULL bằng 0
    SUM(COALESCE(doanh_so_san, 0)) + SUM(COALESCE(doanhThuLadi, 0)) AS doanh_thu_trinh_ads, -- Tổng doanh thu từ trình quảng cáo: doanh thu Ads + doanh thu từ Ladi
    SUM(COALESCE(doanh_so_san, 0)) AS doanhThuAds,
    SUM(COALESCE(doanhThuLadi, 0)) AS doanhThuLadi,
  FROM {{ref("t3_ads_total_with_tkqc")}} tkqc
  left join {{ref("t1_shop_id")}} a  on tkqc.idtkqc = cast(a.idtkqc as string) or tkqc.idtkqc = a.shopid
  
  WHERE chiPhiAds IS NOT NULL
 
  GROUP BY date_start, brand, channel,company,manager,a.shopid,a.shopname--,company_lv1
)
--CTE cir_max_monthly tính toán trung bình chỉ số cir_max
,cir_max_monthly AS (
  SELECT
    year,
    month,
    TRIM(brand) as brand,
    manager,
    TRIM(CONCAT(UPPER(SUBSTR(channel, 1, 1)), LOWER(SUBSTR(channel, 2)))) AS channel,
    AVG(CAST(cir_max AS FLOAT64)) AS avg_cir_max,
    AVG(CAST(cir_max_ads AS FLOAT64)) AS avg_cir_max_ads  -- Lấy trung bình cir_max
  FROM {{ref("t1_cir_max")}}
  GROUP BY year, month, brand, channel,manager
),
cir_max_ads_monthly AS (
  SELECT
    year,
    month,
    TRIM(brand) as brand,
    manager,
    TRIM(CONCAT(UPPER(SUBSTR(channel, 1, 1)), LOWER(SUBSTR(channel, 2)))) AS channel,
    AVG(CAST(cir_max AS FLOAT64)) AS avg_cir_max  -- Lấy trung bình cir_max
  FROM {{ref("t1_cir_max_ads")}}
  GROUP BY year, month, brand, channel,manager
),

revenue_toa AS (
    SELECT 
        DATE(ngay_tao_don) AS date_start,
        brand,
        -- brand_lv1,
        company,
        -- company_lv1,
        channel,
        manager,
        shop_id,
        shop,
        SUM(doanh_thu_ke_toan) AS doanh_thu_ke_toan_toa,
        SUM(tien_chiet_khau_sp ) AS tien_chiet_khau_sp_toa,
        SUM(gia_san_pham_goc_total ) AS gia_san_pham_goc_total_toa,
        SUM(gia_ban_daily_total ) AS gia_ban_daily_total_toa,
        SUM(tien_khach_hang_thanh_toan ) AS tien_khach_hang_thanh_toan_toa,

    FROM {{ref("t3_revenue_all_channel")}}
    where status NOT IN  ('Đã hủy')
    GROUP BY DATE(ngay_tao_don), brand, channel, company,manager,shop_id,shop -- ,company_lv1--,ten_san_pham,sku_code
)
, revenue_tot_total as (
  select  
    TRIM(brand) as brand,
    -- brand_lv1,
    TRIM(CONCAT(UPPER(SUBSTR(channel, 1, 1)), LOWER(SUBSTR(channel, 2)))) AS channel,
    company,
    date(FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(date_create))) as date_start, 
    sum(total_amount) as total_amount
   FROM {{ref("t3_revenue_all_channel_tot")}}
  WHERE date_create IS NOT NULL
 GROUP BY date_start, brand, channel, company
)

, revenue_tot AS (
  SELECT DISTINCT
    TRIM(tot.brand) as brand,
    -- brand_lv1,
    TRIM(CONCAT(UPPER(SUBSTR(tot.channel, 1, 1)), LOWER(SUBSTR(tot.channel, 2)))) AS channel,
    tot.company,
    tot.manager,
    -- company_lv1,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(tot.date_create)) as date_start, 
    
    shop_id,
    shop,
-- Loại bỏ các đơn hàng có tổng_amount nhỏ hơn 60,000
    case
        when SUM(revenue_tot_total.total_amount) < 60000
        then 0
        else SUM(tot.total_amount)
    end as total_amount,
    case
        when SUM(revenue_tot_total.total_amount) < 60000
        then 0
        else  SUM(tot.gia_ban_daily_total)
    end as gia_ban_daily_total,
    case
        when SUM(revenue_tot_total.total_amount) < 60000
        then 0
        else  SUM(tot.doanh_thu_ke_toan)
    end as doanh_thu_ke_toan,
    case
        when SUM(revenue_tot_total.total_amount) < 60000
        then 0
        else  SUM(tot.doanh_thu_ke_toan_v2)
    end as doanh_thu_ke_toan_v2,
    case
        when SUM(revenue_tot_total.total_amount) < 60000
        then 0
        else SUM(tot.tien_chiet_khau_sp_tot) 
    end as tien_chiet_khau_sp_tot,


    -- SUM(tot.total_amount) as total_amount,
    -- SUM(tot.gia_ban_daily_total) as gia_ban_daily_total,
    -- SUM(tot.doanh_thu_ke_toan) as doanh_thu_ke_toan,
    -- SUM(tot.doanh_thu_ke_toan_v2) as doanh_thu_ke_toan_v2,
    -- SUM(tot.tien_chiet_khau_sp_tot) as tien_chiet_khau_sp_tot,


    SUM(phu_phi) as phu_phi
  FROM {{ref("t3_revenue_all_channel_tot")}} tot
  left join revenue_tot_total on  date(FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(tot.date_create))) =  revenue_tot_total.date_start and tot.brand = revenue_tot_total.brand and tot.channel = revenue_tot_total.channel and tot.company = revenue_tot_total.company
  WHERE date_create IS NOT NULL
  GROUP BY date_start, brand, channel, company,manager, shop_id,shop --,company_lv1
)

, a as (select
    coalesce(a.date_start, cast(r_tot.date_start as date), cast(r_toa.date_start as date)) as date_start,
    
    coalesce(a.manager,r_tot.manager, r_toa.manager) as manager,
    coalesce(a.shop_id,r_tot.shop_id, r_toa.shop_id) as shop_id,
    coalesce(a.shop,r_tot.shop, r_toa.shop) as shop,
    coalesce(a.brand, r_tot.brand, r_toa.brand) as brand,
    coalesce(a.channel, r_tot.channel, r_toa.channel) as channel,
    coalesce(a.company, r_tot.company , r_toa.company) as company,
    -- coalesce(a.company_lv1, r_tot.company_lv1 , r_toa.company_lv1) as company_lv1,
    coalesce(a.chi_phi_ads, 0) as chi_phi_ads,
    coalesce(a.doanh_thu_trinh_ads, 0) as doanh_thu_trinh_ads,
    coalesce(a.doanhthuads, 0) as doanhthuads,
    coalesce(a.doanhthuladi, 0) as doanhthuladi,
    extract(year from coalesce(a.date_start, cast(r_tot.date_start as date), cast(r_toa.date_start as date))) as year,
    extract(month from coalesce(a.date_start, cast(r_tot.date_start as date), cast(r_toa.date_start as date))) as month,
    cir_max.avg_cir_max as cir_max,
    cir_max.avg_cir_max_ads as cir_max_ads,
    r_tot.total_amount as total_amount_paid_tot,
    r_tot.gia_ban_daily_total as gia_ban_daily_total_tot,
    r_tot.doanh_thu_ke_toan as doanh_thu_ke_toan_tot,
    r_tot.doanh_thu_ke_toan_v2 as doanh_thu_ke_toan_tot_v2,
    r_tot.tien_chiet_khau_sp_tot,
    r_tot.phu_phi * -1 as phu_phi,
    r_toa.doanh_thu_ke_toan_toa,
    r_toa.tien_chiet_khau_sp_toa,
    r_toa.gia_san_pham_goc_total_toa,
    r_toa.gia_ban_daily_total_toa,
    r_toa.tien_khach_hang_thanh_toan_toa

from revenue_tot r_tot
full outer join 
    revenue_toa r_toa
    on  cast(r_tot.date_start as date) = cast(r_toa.date_start as date)
    and r_tot.brand = r_toa.brand
    and r_tot.channel = r_toa.channel
    and r_tot.company = r_toa.company
    and r_tot.manager = r_toa.manager
    and r_tot.shop_id = r_toa.shop_id
FULL OUTER JOIN ads_daily a
  ON Cast(r_tot.date_start as date) = a.date_start
  AND r_tot.brand = a.brand
  AND r_tot.channel = a.channel
  AND r_tot.company = a.company
  and r_tot.manager = a.manager
  and r_tot.shop_id = a.shop_id
LEFT JOIN cir_max_monthly AS cir_max
  ON EXTRACT(YEAR FROM COALESCE( a.date_start, Cast(r_tot.date_start as date))) = CAST(cir_max.year AS INT64)
  AND EXTRACT(MONTH FROM COALESCE( a.date_start, Cast(r_tot.date_start as date))) = CAST(cir_max.month AS INT64)
  AND COALESCE( a.brand, r_tot.brand) = cir_max.brand
  AND COALESCE( a.channel, r_tot.channel) = cir_max.channel
  and COALESCE( a.manager, r_tot.manager) = cir_max.manager

ORDER BY date_start DESC, brand, channel , manager)

select * from a
