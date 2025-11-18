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
SELECT 
    year,
    month,
    brand,
    channel,
    manager_code,
    manager,
    ma_nhan_vien,
    staff,
    kpi_ds_tong,
    kpi_cir_tong,
    kpi_chi_tieu,
    kpi_ds_moi,
    kpi_cir_moi,
    kpi_ds_cu,
    'Max Eagle' AS company
    FROM `google_sheet.me_kpi` 
    WHERE year IS NOT NULL and month >8 AND role = 'Marketing' 