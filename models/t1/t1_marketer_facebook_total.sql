SELECT 
    manager, 
    staff AS marketing_name,
    marketer_name, 
    ma_nhan_vien, 
    ma_quan_ly, 
    brand,
    'One5' AS company, 
    '' AS team_account,
    start_date,
    end_date
FROM google_sheet.one5_marketer
WHERE TRIM(CONCAT(
    COALESCE(manager, ''), 
    COALESCE(staff, ''), 
    COALESCE(marketer_name, ''),
    COALESCE(ma_nhan_vien, ''), 
    COALESCE(ma_quan_ly, ''), 
    COALESCE(brand, '')
)) <> ''

UNION ALL

SELECT 
    ten_quan_ly AS manager, 
    -- chuẩn hóa marketing_display_name dạng Nguyễn Văn A
    (SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2))) , ' ') FROM UNNEST(SPLIT(marketing_display_name, ' ')) AS word) AS marketing_name,
    marketing_user_name AS marketer_name, 
    id_nhan_vien AS ma_nhan_vien, 
    id_quan_ly AS ma_quan_ly,  
    Brand AS brand,
    'Max Eagle' AS company, 
    team_account,
    start_date,
    end_date
FROM google_sheet.me_marketer
WHERE TRIM(CONCAT(
    COALESCE(ten_quan_ly, ''), 
    COALESCE(marketing_display_name, ''),
    COALESCE(marketing_user_name, ''), 
    COALESCE(id_nhan_vien, ''),
    COALESCE(id_quan_ly, ''), 
    COALESCE(Brand, ''), 
    COALESCE(team_account, '')
)) <> ''
