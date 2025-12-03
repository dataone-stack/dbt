with a as (SELECT 
    brand, 
    -- brand_lv1,
    company,
    sku_code,
    ten_san_pham,
    gia_san_pham_goc_total,

    CAST(ngay_ship AS date) as ngay_ship,
    CAST(ngay_da_giao AS TIMESTAMP) as date_create, 
    ma_don_hang as order_id, 
    status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    phu_phi,
    doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    case
        when order_sources_name  = 'Zalo'
        then 'Zalo'
        else 'Facebook'
    end AS channel,
    phi_van_chuyen_thuc_te,
    gia_von_total,
    gia_von,
    so_luong,
    promotion_type
FROM `crypto-arcade-453509-i8`.`dtm`.`t2_facebook_order_lines_tot`
where CAST(ngay_da_giao AS TIMESTAMP) is not null

union all

SELECT 
    brand, 
    -- brand_lv1,
    company,
    ma_san_pham as sku_code,
    ten_san_pham,
    gia_san_pham_goc_total,

    cast(ngay_ship as date) as ngay_ship,

    ngay_tien_ve_vi as date_create, 
    order_id,
    status, 
    tong_tien_da_thanh_toan as total_amount, 
    ngay_dat_hang as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp_shopee as tien_chiet_khau_sp_tot,
    -- gia_san_pham_goc_total,
    tong_chi_phi *-1 as phu_phi,
    doanh_thu_ke_toan as doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Shopee' AS channel,
    0 as phi_van_chuyen_thuc_te,
    gia_von_total,
    gia_von,
    so_luong,
    promotion_type
FROM `crypto-arcade-453509-i8`.`dtm`.`t2_shopee_order_lines_tot`
-- where status not in ("Đã hủy", "Đang giao")

UNION ALL

SELECT 
    brand, 
    -- brand_lv1,
    company,
    sku_code,
    ten_san_pham,
    gia_san_pham_goc_total,

    CAST(Shipped_Time as date) as ngay_ship,
    CAST(order_statement_time AS TIMESTAMP) as date_create, 
    CAST(ma_don_hang AS STRING) as order_id, 
    -- status, 
    CASE status
      WHEN 'Đã hủy' THEN 'Đã giao thành công'
      WHEN 'Đang giao' THEN 'Đã giao thành công'
      ELSE status
    END AS status,
    total_settlement_amount as total_amount, 
    ngay_tao_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,
    tong_phi_san *-1 as phu_phi,
    doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
    0 as doanh_so_cu,
    0 as doanh_so_moi,
    'Tiktok' AS channel,
    0 as phi_van_chuyen_thuc_te,
    gia_von_total,
    gia_von,
    so_luong,
    promotion_type
FROM `crypto-arcade-453509-i8`.`dtm`.`t2_tiktok_order_line_tot`
-- where status not in ("Đã hủy", "Đang giao")

union all

SELECT 
Case brand
    when 'Chanh tây' then "Cà Phê Mâm Xôi"
    when 'Cà phê gừng' then "Cà Phê Mâm Xôi"
    when 'AMS SLIM' then "Cà Phê Mâm Xôi"
    when 'An Cung' then "LYB Cosmetics"
    when 'Chaching Beauty' then "LYB Cosmetics"
else brand
END as brand, 
    -- brand,
    -- brand_lv1,
    company,
    sku as sku_code,
    san_pham as ten_san_pham,
    thanh_tien as gia_san_pham_goc_total,

    cast(null as date) as ngay_ship,
    ngay_tien_ve_vi as date_create, 
    COALESCE(ma_don_code,CAST(ma_don_so AS STRING))  as order_id, 
    trang_thai_don_hang as status, 
    tien_khach_hang_thanh_toan as total_amount, 
    ngay_chot_don as date_create_order, 
    gia_ban_daily_total,
    tien_chiet_khau_sp as tien_chiet_khau_sp_tot,

    phu_phi,
    doanh_thu_ke_toan,
    0 as doanh_thu_ke_toan_v2,
    doanh_so_cu as doanh_so_cu,
    doanh_so_moi as doanh_so_moi,
    'Facebook' AS channel,
    0 as phi_van_chuyen_thuc_te,
    gia_von_total,
    gia_von,
    so_luong,
    promotion_type
FROM `crypto-arcade-453509-i8`.`dtm`.`t2_mapping_sandbox_pushsale_tot`
),
b as (
  select 
    order_id,
    TRIM(brand) as brand,
    TRIM(CONCAT(UPPER(SUBSTR(channel, 1, 1)), LOWER(SUBSTR(channel, 2)))) AS channel,
    company,
    sum(total_amount) as total_amount_sum,
    sum(gia_ban_daily_total) as gia_ban_daily_total_sum,
    sum(doanh_thu_ke_toan) as doanh_thu_ke_toan_sum,
    sum(tien_chiet_khau_sp_tot) as tien_chiet_khau_sp_tot_sum
  from a
  where EXTRACT(MONTH FROM date_create) >= 6 and EXTRACT(YEAR FROM date_create) >= 2025
  group by order_id, brand, channel, company
)

select
  a.*,
  case 
    when b.total_amount_sum < 60000 then 0
    else a.gia_ban_daily_total
  end as gia_ban_daily_total_final,
  case 
    when b.total_amount_sum < 60000 then 0
    else a.doanh_thu_ke_toan
  end as doanh_thu_ke_toan_final,
  case 
    when b.total_amount_sum < 60000 then 0
    else a.tien_chiet_khau_sp_tot
  end as tien_chiet_khau_sp_tot_final
from a
left join b
  on a.order_id = b.order_id
  and TRIM(a.brand) = b.brand
  and TRIM(CONCAT(UPPER(SUBSTR(a.channel, 1, 1)), LOWER(SUBSTR(a.channel, 2)))) = b.channel
  and a.company = b.company
where EXTRACT(MONTH FROM a.date_create) >= 6 and EXTRACT(YEAR FROM a.date_create) >= 2025