SELECT * ,
    'Max Eagle' AS company
    FROM `google_sheet.me_kpi` 
    WHERE year IS NOT NULL and month >10 AND role = 'Sales/CS' 