WITH 

-- Lấy tháng đầu kỳ từng nhân viên (cột year, month)
opening_month AS (
  SELECT staff_code, year, month
  FROM {{ ref('t1_financial_debt') }}
  WHERE month = 6  -- hoặc tháng đầu kỳ theo dữ liệu bạn
),

-- Tạo danh sách month - staff_code, chỉ lấy tháng >= tháng đầu kỳ của nhân viên
all_months AS (
  SELECT DISTINCT yms.year, yms.month, yms.staff_code, yms.staff_name
  FROM (
    SELECT year, month, staff_code, staff_name FROM {{ ref('t1_financial_debt') }}
    UNION DISTINCT
    SELECT year, month, staff_code, staff_name FROM {{ ref('t1_financial_incurring_debt') }}
    UNION DISTINCT
    SELECT year, month, ma_quan_ly as staff_code, manager as staff_name FROM {{ ref('t3_financial_invoice_ads') }}
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
    am.staff_name,
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
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) - IFNULL(chi_phi_ads,0)) AS month_change_chiphi_ads,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) - IFNULL(spend,0)) AS month_change_spend,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) - IFNULL(invoice,0)) AS month_change_invoice,
    (IFNULL(incurring_debt,0) - IFNULL(incurring_available,0) - IFNULL(amount,0)) AS month_change_amount
  FROM base
),

calculations AS (
  SELECT *,
    -- Tính số dư lũy kế theo từng loại chi phí
    SUM(initial_debt + month_change_chiphi_ads) OVER (
      PARTITION BY staff_code 
      ORDER BY year, month 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS closing_balance_chiphi_ads,

    SUM(initial_debt + month_change_spend) OVER (
      PARTITION BY staff_code 
      ORDER BY year, month 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS closing_balance_spend,

    SUM(initial_debt + month_change_invoice) OVER (
      PARTITION BY staff_code 
      ORDER BY year, month 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS closing_balance_invoice,

    SUM(initial_debt + month_change_amount) OVER (
      PARTITION BY staff_code 
      ORDER BY year, month 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS closing_balance_amount
  FROM trans
)

SELECT
  year, month, staff_code, staff_name, company,

  -- Opening balance = closing balance của tháng trước (hoặc initial_debt nếu là tháng đầu)
  COALESCE(
    LAG(closing_balance_chiphi_ads) OVER (PARTITION BY staff_code ORDER BY year, month),
    initial_debt
  ) AS opening_balance_chiphi_ads,

  COALESCE(
    LAG(closing_balance_spend) OVER (PARTITION BY staff_code ORDER BY year, month),
    initial_debt
  ) AS opening_balance_spend,

  COALESCE(
    LAG(closing_balance_invoice) OVER (PARTITION BY staff_code ORDER BY year, month),
    initial_debt
  ) AS opening_balance_invoice,

  COALESCE(
    LAG(closing_balance_amount) OVER (PARTITION BY staff_code ORDER BY year, month),
    initial_debt
  ) AS opening_balance_amount,

  -- Số phát sinh trong tháng
  net_incurring AS net_incurring_only,
  chi_phi_ads, spend, invoice, amount,

  -- Số dư cuối kỳ
  closing_balance_chiphi_ads,
  closing_balance_spend,
  closing_balance_invoice,
  closing_balance_amount

FROM calculations
ORDER BY staff_code, year, month
