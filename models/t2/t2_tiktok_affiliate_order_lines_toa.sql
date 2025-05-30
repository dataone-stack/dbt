with tiktok_aff_order as(

  select
    DATE(DATE_ADD(aff.create_time, INTERVAL 427 MINUTE)) as ngay_tao_don,
    aff.order_id,
    aff.brand,
    ord.payment_method_name,
    aff.status,
    safe_cast(json_value(json, '$.price.amount') as int64) as tong_tien_sp,
    case
      when safe_cast(json_value(json, '$.returned_quantity') as int64) > 0
      then safe_cast(json_value(json, '$.price.amount') as int64)
      else 0
    end as so_hoan,
    safe_cast(json_value(json, '$.quantity') as int64) as so_luong,
  
    json_value(json, '$.content_type') as loai_noi_dung,
    json_value(json, '$.creator_username') as ten_nguoi_dung_nha_sang_tao,
    COALESCE(safe_cast(json_value(json, '$.estimated_paid_commission.amount') as int64),0)as thanh_toan_hoa_hong_tieu_chuan_uoc_tinh,
    COALESCE(safe_cast(json_value(json, '$.estimated_paid_shop_ads_commission.amount') as int64),0) as thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh

  from {{ref("t1_tiktok_affiliate_order_total")}} as aff,
  unnest(aff.skus) as json
  LEFT JOIN {{ref("t1_tiktok_order_tot")}} as ord
  on aff.order_id = ord.order_id and aff.brand = ord.brand
  Left join {{ref("t1_tiktok_order_tot")}} as ord

)

SELECT 
  *,
  thanh_toan_hoa_hong_tieu_chuan_uoc_tinh + thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh as hoa_hong_uoc_tinh,
  tong_tien_sp/so_luong as gia_sp,
  tong_tien_sp - so_hoan as tong_tien_san_pham_sau_khi_tru_hoan,
  case
  when thanh_toan_hoa_hong_tieu_chuan_uoc_tinh > 0 and thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh = 0
  then 'Organic'
  when thanh_toan_hoa_hong_tieu_chuan_uoc_tinh = 0 and thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh > 0
  then 'Ads'
  else '-'
  end as organic_ads
from tiktok_aff_order


