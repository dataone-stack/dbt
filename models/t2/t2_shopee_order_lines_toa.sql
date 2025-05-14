with return_detail as(
SELECT 
order_id, 
i.variation_sku, 
i.amount * item_price as so_tien_hoan_tra
  FROM {{ref("t1_shopee_shop_order_retrurn_total")}},
  UNNEST(item) as i
),

total_amount AS (
    SELECT 
        order_id,
        SUM(i.discounted_price) AS total_tong_tien_san_pham
    FROM {{ref("t1_shopee_shop_fee_total")}},   
    UNNEST(items) AS i

    GROUP BY order_id

),

sale_detail as(
 select 
    detail.order_id,
    detail.buyer_user_name as ten_nguoi_mua,
    i.model_sku,
    i.item_name,
    i.model_name,
    i.quantity_purchased,
    i.discounted_price,
    COALESCE(rd.so_tien_hoan_tra, 0) as so_tien_hoan_tra,
    (i.discounted_price) as tong_tien_san_pham,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.discounted_price) / ta.total_tong_tien_san_pham) * detail.commission_fee ELSE 0 END, 0) as phi_co_dinh,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.discounted_price) / ta.total_tong_tien_san_pham) * detail.service_fee ELSE 0 END, 0) as phi_dich_vu,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.discounted_price) / ta.total_tong_tien_san_pham) * detail.seller_transaction_fee ELSE 0 END, 0) as phi_thanh_toan,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.discounted_price) / ta.total_tong_tien_san_pham) * detail.actual_shipping_fee ELSE 0 END, 0) as phi_van_chuyen_thuc_te,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.discounted_price) / ta.total_tong_tien_san_pham) * detail.shopee_shipping_rebate ELSE 0 END, 0) as phi_van_chuyen_tro_gia_tu_shopee,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.discounted_price) / ta.total_tong_tien_san_pham) * detail.credit_card_promotion ELSE 0 END, 0) as khuyen_mai_cho_the_tin_dung,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.discounted_price) / ta.total_tong_tien_san_pham) * detail.order_ams_commission_fee ELSE 0 END, 0) as phi_hoa_hong_tiep_thi_lien_ket,
    i.discount_from_voucher_shopee as shopee_voucher,
    i.discount_from_coin,
    i.discount_from_voucher_seller,
    i.shopee_discount
  from {{ref("t1_shopee_shop_fee_total")}} as detail,
  unnest (items) as i
  left join return_detail rd on detail.order_id = rd.order_id and i.model_sku = rd.variation_sku
  left join total_amount ta on ta.order_id = detail.order_id
),

sale_order_detail as (
  select
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) as create_time,
    ord.order_status,
    ord.payment_method as hinh_thuc_thanh_toan,
    ord.shipping_carrier as ten_don_vi_van_chuyen,
    sd.*,
    COALESCE((sd.tong_tien_san_pham/ta.total_tong_tien_san_pham)*ord.total_amount,0) as total_amount
  from sale_detail as sd
  left join {{ref("t1_shopee_shop_order_detail_total")}} as ord
  on sd.order_id = ord.order_id
  left join total_amount ta on ta.order_id = sd.order_id
)

select
  create_time,
  ten_nguoi_mua,
  hinh_thuc_thanh_toan,
  ten_don_vi_van_chuyen,
  order_status,
  order_id,
  item_name,
  model_name,
  model_sku,
  quantity_purchased,
  tong_tien_san_pham,
  total_amount,
  so_tien_hoan_tra,
  phi_van_chuyen_thuc_te,
  phi_van_chuyen_tro_gia_tu_shopee,
  phi_co_dinh,
  phi_dich_vu,
  phi_thanh_toan,
  phi_hoa_hong_tiep_thi_lien_ket,
  round(tong_tien_san_pham - phi_hoa_hong_tiep_thi_lien_ket- phi_van_chuyen_thuc_te + phi_van_chuyen_tro_gia_tu_shopee - phi_co_dinh - phi_thanh_toan - phi_dich_vu) as doanh_thu_don_hang_uoc_tinh,
  shopee_voucher,
  discount_from_coin,
  discount_from_voucher_seller,
  khuyen_mai_cho_the_tin_dung,
  round(tong_tien_san_pham - shopee_voucher - discount_from_coin - discount_from_voucher_seller - khuyen_mai_cho_the_tin_dung ) as tong_tien_thanh_toan
from sale_order_detail