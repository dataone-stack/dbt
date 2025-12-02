select 
SAFE_CAST(year as INT64) as year, 
SAFE_CAST(month as INT64) as month, 
brand, 
layer1, 
layer2, 
SAFE_CAST(total as FLOAT64) as total, 
type_of_calculation, 
company
 from crypto-arcade-453509-i8.google_sheet.chi_phi_co_dinh_pl
-- WHERE ma_sku IS NOT NULL