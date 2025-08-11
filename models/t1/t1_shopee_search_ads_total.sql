SELECT *
FROM google_sheet.shopee_search_temp
--loại bỏ các dòng trống trong ggsheet
WHERE TRIM(CONCAT(
    COALESCE(CAST(account_id AS STRING), ''), 
    COALESCE(CAST(doanhThuAds AS STRING), ''), 
    COALESCE(CAST(chiphi AS STRING), ''), 
    COALESCE(CAST(date_start AS STRING), '')
)) <> ''


-- select * from google_sheet.shopee_search_temp
