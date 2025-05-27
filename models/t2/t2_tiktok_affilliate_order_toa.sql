with tiktok_aff_order as(
  select
    DATE_ADD(aff.create_time, INTERVAL 427 MINUTE) as ngay_tao_don,
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
    COALESCE(safe_cast(json_value(json, '$.estimated_paid_shop_ads_commission.amount') as int64),0) as thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh,
    case
        when COALESCE(safe_cast(json_value(json, '$.estimated_paid_commission.amount') as int64),0) > 0 and COALESCE(safe_cast(json_value(json, '$.estimated_paid_shop_ads_commission.amount') as int64),0) = 0
        then 'Organic'
        when COALESCE(safe_cast(json_value(json, '$.estimated_paid_commission.amount') as int64),0) = 0 and COALESCE(safe_cast(json_value(json, '$.estimated_paid_shop_ads_commission.amount') as int64),0) > 0
        then 'Ads'
        else '-'
    end as organic_ads

  from {{ref("t1_tiktok_affiliate_order_total")}} as aff,
  unnest(aff.skus) as json
  LEFT JOIN {{ref("t1_tiktok_order_tot")}} as ord
  on aff.order_id = ord.order_id and aff.brand = ord.brand


)

SELECT 
  ngay_tao_don,
  order_id,
  brand,
  payment_method_name as phuong_thuc_thanh_toan,
  status as trang_thai_don_hang,
  loai_noi_dung,
  ten_nguoi_dung_nha_sang_tao,
  thanh_toan_hoa_hong_tieu_chuan_uoc_tinh,
  organic_ads,
  sum(so_hoan) as so_hoan,
  sum(so_luong) as quantity,
  sum(thanh_toan_hoa_hong_tieu_chuan_uoc_tinh) as thanh_toan_hoa_hong_tieu_chuan_uoc_tinh,
  sum(thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh) as thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh,

  sum(thanh_toan_hoa_hong_tieu_chuan_uoc_tinh + thanh_toan_hoa_hong_quang_cao_cua_hang_uoc_tinh) as hoa_hong_uoc_tinh,
  
  sum(tong_tien_sp - so_hoan) as tong_tien_san_pham_sau_khi_tru_hoan,
  
from tiktok_aff_order
group by
    ngay_tao_don,
    order_id,
    brand,
    payment_method_name as phuong_thuc_thanh_toan,
    status as trang_thai_don_hang,
    loai_noi_dung,
    ten_nguoi_dung_nha_sang_tao,
    thanh_toan_hoa_hong_tieu_chuan_uoc_tinh,
    organic_ads