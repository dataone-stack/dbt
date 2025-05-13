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
        SUM(i.discounted_price*i.quantity_purchased) AS total_tong_tien_san_pham
    FROM {{ref("t1_shopee_shop_fee_total")}},   
    UNNEST(items) AS i

    GROUP BY order_id

),

sale_detail as(
 select 
    detail.order_id,
    i.model_sku,
    i.item_name,
    i.model_name,
    i.quantity_purchased,
    i.discounted_price,
    COALESCE(rd.so_tien_hoan_tra, 0) as so_tien_hoan_tra,
    (i.quantity_purchased * i.discounted_price) as tong_tien_san_pham,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.quantity_purchased * i.discounted_price) / ta.total_tong_tien_san_pham) * detail.commission_fee ELSE 0 END, 0) as phi_co_dinh,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.quantity_purchased * i.discounted_price) / ta.total_tong_tien_san_pham) * detail.service_fee ELSE 0 END, 0) as phi_dich_vu,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.quantity_purchased * i.discounted_price) / ta.total_tong_tien_san_pham) * detail.seller_transaction_fee ELSE 0 END, 0) as phi_thanh_toan,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.quantity_purchased * i.discounted_price) / ta.total_tong_tien_san_pham) * detail.actual_shipping_fee ELSE 0 END, 0) as phi_van_chuyen_thuc_te,
    COALESCE(CASE WHEN COALESCE(rd.so_tien_hoan_tra, 0) = 0 THEN ((i.quantity_purchased * i.discounted_price) / ta.total_tong_tien_san_pham) * detail.shopee_shipping_rebate ELSE 0 END, 0) as phi_van_chuyen_tro_gia_tu_shopee,
    i.discount_from_voucher_shopee as shopee_voucher
  from {{ref("t1_shopee_shop_fee_total")}} as detail,
  unnest (items) as i
  left join return_detail rd on detail.order_id = rd.order_id and i.model_sku = rd.variation_sku
  left join total_amount ta on ta.order_id = detail.order_id
),

sale_order_detail as (
  select
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) as create_time,
    ord.order_status,
    sd.*
   
  from sale_detail as sd
  left join {{ref("t1_shopee_shop_order_detail_total")}} as ord
  on sd.order_id = ord.order_id
)

select
  create_time,
  order_status,
  order_id,
  item_name,
  model_name,
  model_sku,
  quantity_purchased,
  discounted_price,
  tong_tien_san_pham,
  so_tien_hoan_tra,
  phi_van_chuyen_thuc_te,
  phi_van_chuyen_tro_gia_tu_shopee,
  phi_co_dinh,
  phi_dich_vu,
  phi_thanh_toan,
  round(tong_tien_san_pham -so_tien_hoan_tra- phi_van_chuyen_thuc_te + phi_van_chuyen_tro_gia_tu_shopee - phi_co_dinh - phi_thanh_toan - phi_dich_vu) as doanh_thu_don_hang_uoc_tinh,
  shopee_voucher,
  round(tong_tien_san_pham - shopee_voucher - so_tien_hoan_tra) as tong_tien_thanh_toan
from sale_order_detail