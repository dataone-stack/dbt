

with
    ads_daily as (
        select
            date_start,
            brand,
            brand_lv1,
            channel,
            company,
            sum(coalesce(chiphiads, 0)) as chi_phi_ads,
            sum(coalesce(doanhthuads, 0))
            + sum(coalesce(doanhthuladi, 0)) as doanh_thu_trinh_ads,
            sum(coalesce(doanhthuads, 0)) as doanhthuads,
            sum(coalesce(doanhthuladi, 0)) as doanhthuladi,
        from {{ ref("t3_ads_total_with_tkqc") }}
        where chiphiads is not null
        group by date_start, brand, brand_lv1, channel, company
    ),

    cir_max_monthly as (
        select
            year,
            month,
            brand,
            channel,
            avg(cast(cir_max as float64)) as avg_cir_max,
            avg(cast(cir_max_ads as float64)) as avg_cir_max_ads  -- Lấy trung bình cir_max
        from {{ ref("t1_cir_max") }}
        group by year, month, brand, channel
    ),

    cir_max_ads_monthly as (
        select year, month, brand, channel, avg(cast(cir_max as float64)) as avg_cir_max  -- Lấy trung bình cir_max
        from {{ ref("t1_cir_max_ads") }}
        group by year, month, brand, channel
    ),

    revenue_toa AS (
        SELECT 
            DATE(ngay_tao_don) AS date_start,
            brand,
            brand_lv1,
            company,
            channel,
            SUM(doanh_thu_ke_toan) AS doanh_thu_ke_toan_toa,
            SUM(tien_chiet_khau_sp ) AS tien_chiet_khau_sp_toa,
            SUM(gia_san_pham_goc_total ) AS gia_san_pham_goc_total_toa,
            SUM(gia_ban_daily_total ) AS gia_ban_daily_total_toa,

        FROM {{ ref('t3_revenue_all_channel') }}
        -- WHERE status NOT IN  ('Đã hủy')
        GROUP BY DATE(ngay_tao_don), brand, brand_lv1, channel, company --,ten_san_pham,sku_code
    ),  

    revenue_tot as (
        select distinct
            brand,
            brand_lv1,
            company,
            date(format_timestamp('%Y-%m-%d', timestamp(date_create))) as date_start,
            
            case
                when sum(total_amount) < 60000 then 0 else sum(total_amount)
            end as total_amount,
            case
                when sum(total_amount) < 60000 then 0 else sum(gia_ban_daily_total)
            end as gia_ban_daily_total,
            case
                when sum(total_amount) < 60000 then 0 else sum(doanh_thu_ke_toan)
            end as doanh_thu_ke_toan,
            channel,
            case
                when sum(total_amount) < 60000 then 0 else sum(tien_chiet_khau_sp_tot)
            end as tien_chiet_khau_sp_tot,
            sum(phu_phi) as phu_phi
        from {{ ref("t3_revenue_all_channel_tot") }}
        where date_create is not null
        group by date(format_timestamp('%Y-%m-%d', timestamp(date_create))), brand, brand_lv1, channel, company
    )
select
    coalesce(a.date_start, cast(r_tot.date_start as date), cast(r_toa.date_start as date)) as date_start,
    coalesce(a.brand, r_tot.brand, r_toa.brand) as brand,
    coalesce(a.brand_lv1, r_tot.brand_lv1, r_toa.brand_lv1) as brand_lv1,
    coalesce(a.channel, r_tot.channel, r_toa.channel) as channel,
    coalesce(a.company, r_tot.company , r_toa.company) as company,
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
    r_tot.tien_chiet_khau_sp_tot,
    r_tot.phu_phi,
    r_toa.doanh_thu_ke_toan_toa,
    r_toa.tien_chiet_khau_sp_toa,
    r_toa.gia_san_pham_goc_total_toa,
    r_toa.gia_ban_daily_total_toa,

from revenue_tot r_tot
full outer join 
    revenue_toa r_toa
    on  cast(r_tot.date_start as date) = cast(r_toa.date_start as date)
    and r_tot.brand = r_toa.brand
    and r_tot.brand_lv1 = r_toa.brand_lv1
    and r_tot.channel = r_toa.channel
    and r_tot.company = r_toa.company
full outer join
    ads_daily a
    on cast(r_tot.date_start as date) = a.date_start
    and r_tot.brand = a.brand
    and r_tot.brand_lv1 = a.brand_lv1
    and r_tot.channel = a.channel
    and r_tot.company = a.company
left join
    cir_max_monthly as cir_max
    on extract(year from coalesce(a.date_start, cast(r_tot.date_start as date)))
    = cast(cir_max.year as int64)
    and extract(month from coalesce(a.date_start, cast(r_tot.date_start as date)))
    = cast(cir_max.month as int64)
    and coalesce(a.brand, r_tot.brand) = cir_max.brand
    and coalesce(a.channel, r_tot.brand) = cir_max.channel
order by
    date_start desc,
    brand,
    brand_lv1,
    channel