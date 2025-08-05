WITH 

-- Lấy tháng đầu kỳ từng nhân viên (cột year, month)
opening_month AS (
  SELECT staff_code, year, month
  FROM `dtm.t1_financial_debt`
  WHERE month = 6  -- hoặc tháng đầu kỳ theo dữ liệu bạn
),

-- Tạo danh sách month - staff_code, chỉ lấy tháng >= tháng đầu kỳ của nhân viên
all_months AS (
  SELECT DISTINCT yms.year, yms.month, yms.staff_code
  FROM (
    SELECT year, month, staff_code FROM {{ ref('t1_financial_debt') }}
    UNION DISTINCT
    SELECT year, month, staff_code FROM {{ ref('t1_financial_incurring_debt') }}
    UNION DISTINCT
    SELECT year, month, ma_quan_ly AS staff_code FROM {{ ref('t3_financial_invoice_ads') }}
  ) yms
  JOIN opening_month om 
    ON yms.staff_code = om.staff_code
    AND (yms.year > om.year OR (yms.year = om.year AND yms.month >= om.month))
),

base AS (
  SELECT 
    am.year,
    am.month,
    am.staff_code,
    COALESCE(debt.company, inc.company, cost.company) AS company,
    IFNULL(debt.opening_balance_debt, 0) AS opening_balance_debt,
    IFNULL(debt.opening_balance_available, 0) AS opening_balance_available,
    IFNULL(inc.opening_balance_debt, 0) AS incurring_debt,
    IFNULL(inc.opening_balance_available, 0) AS incurring_available,
    IFNULL(cost.chi_phi_ads, 0) AS chi_phi_ads,
    IFNULL(cost.spend, 0) AS spend,
    IFNULL(cost.invoice, 0) AS invoice,
    IFNULL(cost.amount, 0) AS amount
  FROM all_months am
  LEFT JOIN {{ ref('t1_financial_debt') }} debt 
    ON am.year = debt.year AND am.month = debt.month AND am.staff_code = debt.staff_code
  LEFT JOIN {{ ref('t1_financial_incurring_debt') }} inc 
    ON am.year = inc.year AND am.month = inc.month AND am.staff_code = inc.staff_code
  LEFT JOIN (
    SELECT year, month, ma_quan_ly AS staff_code, company,
      SUM(IFNULL(chi_phi_ads, 0)) AS chi_phi_ads,
      SUM(IFNULL(spend, 0)) AS spend,
      SUM(IFNULL(invoice, 0)) AS invoice,
      SUM(IFNULL(amount, 0)) AS amount
    FROM {{ ref('t3_financial_invoice_ads') }}
    GROUP BY year, month, ma_quan_ly, company
  ) cost 
    ON am.year = cost.year AND am.month = cost.month AND am.staff_code = cost.staff_code
),

trans AS (
  SELECT *,
    CASE WHEN month = 6 THEN (opening_balance_debt - opening_balance_available) ELSE 0 END AS initial_debt,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0)) AS net_incurring,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) + IFNULL(chi_phi_ads,0)) AS month_add_chiphi_ads,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) + IFNULL(spend,0)) AS month_add_spend,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) + IFNULL(invoice,0)) AS month_add_invoice,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) + IFNULL(amount,0)) AS month_add_amount
  FROM base
)

SELECT
  year, month, staff_code, company,

  -- Số tiền đầu kỳ tháng 6 (chỉ hiển thị ở tháng 6, các tháng khác = 0)
  initial_debt AS opening_balance,

  -- Số phát sinh ròng trong tháng (incurring + tổng chi phí)
  net_incurring AS net_incurring_only,

  -- Các khoản phát sinh riêng từng loại chi phí của tháng đó
  chi_phi_ads, spend, invoice, amount,

    initial_debt + net_incurring - chi_phi_ads AS closing_balance_chiphi_ads,
    initial_debt + net_incurring - spend AS closing_balance_spend,
    initial_debt + net_incurring - invoice AS closing_balance_invoice,
    initial_debt + net_incurring - amount AS closing_balance_amount,

FROM trans
ORDER BY staff_code, year, month
