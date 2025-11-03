SELECT 
    CAST(idtkqc AS STRING) AS idtkqc, 
    nametkqc, 
    ben_thue, 
    phi_thue, 
    CAST(dau_the AS INT64) AS dau_the, 
    ma_nhan_vien, 
    -- chuẩn hóa staff dạng Nguyễn Văn A
    (SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2))) , ' ') FROM UNNEST(SPLIT(staff, ' ')) AS word) AS staff,
    ma_quan_ly, 
    manager, 
    brand, 
    channel, 
    status, 
    start_date, 
    end_date, 
    'One5' AS company 
FROM `google_sheet.tkqc` 
WHERE idtkqc IS NOT NULL

UNION ALL

SELECT 
    CAST(idtkqc AS STRING) AS idtkqc, 
    nametkqc, 
    ben_thue, 
    phi_thue, 
    case
    when dau_the is null
    then 0
    else CAST(dau_the AS INT64) 
    end as dau_the,
    ma_nhan_vien, 
    -- chuẩn hóa staff dạng Nguyễn Văn A
    (SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2))) , ' ') FROM UNNEST(SPLIT(staff, ' ')) AS word) AS staff,
    ma_quan_ly, 
    manager, 
    brand, 
    channel, 
    status, 
    start_date, 
    end_date,
    'Max Eagle' AS company 
FROM `google_sheet.tkqc_me` 
WHERE idtkqc IS NOT NULL and channel not in ('Google Ads','Marketplcae','Tiktok shop')