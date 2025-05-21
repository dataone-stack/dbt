WITH return_detail AS (
  SELECT 
    order_id,
    return_id,
    brand,
    update_time,
    i.variation_sku, 
    i.item_price * i.amount AS so_tien_hoan_tra,
    status,
    refund_amount
  FROM {{ref("t1_shopee_shop_order_retrurn_total")}},
  UNNEST(item) AS i
),

total_amount AS (
  SELECT 
    order_id,
    brand,
    SUM(i.discounted_price) AS total_tong_tien_san_pham
  FROM {{ref("t1_shopee_shop_fee_total")}},   
  UNNEST(items) AS i
  GROUP BY order_id,brand
),

sale_detail AS (
  SELECT 
    detail.order_id,
    detail.buyer_user_name AS ten_nguoi_mua,
    detail.brand,
    i.model_sku,
    i.item_name,
    i.model_name,
    i.quantity_purchased,
    (i.original_price / i.quantity_purchased) AS gia_san_pham_goc,
    i.discounted_price,
    rd.update_time AS ngay_return,
    vi.create_time AS ngay_tien_ve_vi,
    CASE
      WHEN DATE(rd.update_time) >= DATE(vi.create_time) or rd.refund_amount = 0
      THEN rd.so_tien_hoan_tra
      ELSE 0
    END AS so_tien_hoan_tra,
    (i.discounted_price) AS tong_tien_san_pham,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.buyer_paid_shipping_fee AS phi_van_chuyen_nguoi_mua_tra,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.commission_fee AS phi_co_dinh,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.service_fee AS phi_dich_vu,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.seller_transaction_fee  AS phi_thanh_toan,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.actual_shipping_fee  AS phi_van_chuyen_thuc_te,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.shopee_shipping_rebate  AS phi_van_chuyen_tro_gia_tu_shopee,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.credit_card_promotion  AS khuyen_mai_cho_the_tin_dung,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.order_ams_commission_fee  AS phi_hoa_hong_tiep_thi_lien_ket,
    (i.discounted_price / ta.total_tong_tien_san_pham) * detail.voucher_from_seller  AS voucher_from_seller,
    i.discount_from_voucher_shopee AS shopee_voucher,
    i.discount_from_coin,
    i.discount_from_voucher_seller,
    case
        when rd.return_id is not null
        then 0
        else i.shopee_discount
    end AS tro_gia_tu_shopee
  FROM {{ref("t1_shopee_shop_fee_total")}} AS detail,
  UNNEST(items) AS i
  LEFT JOIN return_detail rd ON detail.order_id = rd.order_id AND i.model_sku = rd.variation_sku and detail.brand = rd.brand and rd.status = 'ACCEPTED'
  LEFT JOIN total_amount ta ON ta.order_id = detail.order_id and ta.brand = detail.brand
  LEFT JOIN {{ref("t1_shopee_shop_wallet_total")}} vi ON detail.order_id = vi.order_id and detail.brand = vi.brand and vi.transaction_tab_type = 'wallet_order_income'
),

sale_order_detail AS (
  SELECT
    sd.*,
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) AS create_time,
    ord.order_status,
    ord.payment_method AS hinh_thuc_thanh_toan,
    ord.shipping_carrier AS ten_don_vi_van_chuyen,
    ord.ship_by_date AS ngay_ship,
    ord.buyer_cancel_reason AS ly_do_huy_don,
    COALESCE(((sd.discounted_price) / ta.total_tong_tien_san_pham) * ord.days_to_ship, 0) AS day_to_ship,
    COALESCE(((sd.discounted_price) / ta.total_tong_tien_san_pham) * ord.total_amount, 0) AS test_doanh_thu
  FROM sale_detail AS sd
  LEFT JOIN {{ref("t1_shopee_shop_order_detail_total")}} AS ord
    ON sd.order_id = ord.order_id and sd.brand = ord.brand
  LEFT JOIN total_amount ta ON ta.order_id = sd.order_id and ta.brand = sd.brand
)

SELECT
  test_doanh_thu,
  create_time,
  ten_nguoi_mua,
  brand,
  ngay_return,
  ngay_tien_ve_vi,
  hinh_thuc_thanh_toan,
  ten_don_vi_van_chuyen,
  ngay_ship,
  day_to_ship,
  ly_do_huy_don,
  order_status,
  order_id,
  item_name,
  model_name,
  model_sku,
  quantity_purchased,
  gia_san_pham_goc,
  tong_tien_san_pham,
  so_tien_hoan_tra,
  phi_van_chuyen_nguoi_mua_tra,
  phi_van_chuyen_thuc_te,
  phi_van_chuyen_tro_gia_tu_shopee,
  phi_co_dinh,
  phi_dich_vu,
  phi_thanh_toan,
  phi_hoa_hong_tiep_thi_lien_ket,
  tro_gia_tu_shopee,
  voucher_from_seller,
  ROUND(tong_tien_san_pham - so_tien_hoan_tra -voucher_from_seller + phi_van_chuyen_nguoi_mua_tra -phi_van_chuyen_thuc_te + phi_van_chuyen_tro_gia_tu_shopee + tro_gia_tu_shopee - phi_co_dinh - phi_dich_vu - phi_thanh_toan - phi_hoa_hong_tiep_thi_lien_ket ) AS doanh_thu_don_hang_uoc_tinh,
  shopee_voucher,
  discount_from_coin,
  discount_from_voucher_seller,
  khuyen_mai_cho_the_tin_dung,
  ROUND(tong_tien_san_pham - shopee_voucher - discount_from_coin - discount_from_voucher_seller - khuyen_mai_cho_the_tin_dung) AS tong_tien_thanh_toan
FROM sale_order_detail 