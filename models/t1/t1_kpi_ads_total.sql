SELECT
    year,
    month,
    brand,
    channel,
    manager_code,
    manager,
    ma_nhan_vien,
    staff,
    revenue_target AS kpi_ds_tong,
    cir_target AS kpi_cir_tong,
    spend AS kpi_chi_tieu,
    0 AS kpi_ds_moi,
    0 AS kpi_cir_moi,
    0 AS kpi_ds_cu,
    'One5' AS company
    FROM `google_sheet.one5_kpi_ads`
    WHERE year IS NOT NULL 
UNION ALL
SELECT * ,
    'Max Eagle' AS company
    FROM `google_sheet.me_kpi_ad`
    WHERE year IS NOT NULL and month >8