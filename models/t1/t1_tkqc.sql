SELECT 
    CAST(idtkqc AS STRING) AS idtkqc, 
    nametkqc, 
    ben_thue, 
    phi_thue, 
    CAST(dau_the AS INT64) AS dau_the, 
    ma_nhan_vien, 
    staff, 
    ma_quan_ly, 
    manager, 
    brand, 
    channel, 
    status, 
    start_date, 
    end_date, 
    'Team A Tiến' AS company 
FROM `google_sheet.tkqc` 
WHERE idtkqc IS NOT NULL

UNION ALL

SELECT 
    CAST(idtkqc AS STRING) AS idtkqc, 
    nametkqc, 
    ben_thue, 
    phi_thue, 
    CAST(dau_the AS INT64) AS dau_the, 
    ma_nhan_vien, 
    staff, 
    ma_quan_ly, 
    manager, 
    brand, 
    channel, 
    status, 
    start_date, 
    end_date,
    'Max Eagle' AS company 
FROM `google_sheet.tkqc_me` 
WHERE idtkqc IS NOT NULL and channel not in ('Google Ads','Marketplcae','Shopee','Tiktok shop')