with total_lead as (
  select ngay_tao_contact,ma_quan_ly,sum(so_lead) as total_lead from {{ref("t2_sandbox_sale_toa")}} group by ngay_tao_contact,ma_quan_ly
),
 t2_sale as (
  select * from {{ref("t2_sandbox_sale_toa")}}
),
t3_ads as (
  select date_start,ma_quan_ly, sum(chiPhiAds) as chiPhiAds  from {{ref("t3_ads_total_with_tkqc")}} group by date_start,ma_quan_ly
)
, a as(
  select
  sale.*,
  safe_divide(sale.so_lead , total.total_lead) * ads.chiPhiAds as chiPhiLead


  from t2_sale sale
  left join t3_ads ads on ads.ma_quan_ly = sale.ma_quan_ly and date(ads.date_start) = date(sale.ngay_tao_contact)
  left join total_lead total on total.ma_quan_ly = sale.ma_quan_ly and date(total.ngay_tao_contact) = date(sale.ngay_tao_contact)

)

select * from a

