SELECT *,
    'One5' AS company
    FROM `google_sheet.one5_kpi_ads`
    WHERE year IS NOT NULL
UNION ALL
SELECT * ,
    'Max Eagle' AS company
    FROM `google_sheet.me_kpi_ads`
    WHERE year IS NOT NULL