

WITH return_detail AS (
  SELECT 
    order_id,
    return_id,
    brand,
    update_time,
    i.variation_sku, 
    i.item_price * i.amount AS so_tien_hoan_tra,
    status,
    refund_amount,
    return_seller_due_date
  FROM {{ ref('t1_shopee_shop_order_retrurn_total') }},
  UNNEST(item) AS i
),

total_amount AS (
  SELECT 
    order_id,
    brand,
    SUM(i.discounted_price) AS total_tong_tien_san_pham
  FROM {{ ref('t1_shopee_shop_fee_total') }},   
  UNNEST(items) AS i
  GROUP BY order_id,brand
),

sale_detail AS (
  SELECT 
    detail.order_id,
    detail.buyer_user_name AS ten_nguoi_mua,
    detail.brand,
    i.model_sku,
    i.item_sku,
    i.item_name,
    i.model_name,
    i.quantity_purchased,
    (i.original_price/i.quantity_purchased) AS gia_san_pham_goc,
    seller_discount AS nguoi_ban_tro_gia,
    i.discounted_price,
    (i.original_price/i.quantity_purchased) as test_doanh_thu,
    rd.update_time AS ngay_return,
    vi.create_time AS ngay_tien_ve_vi,
    CASE
      WHEN DATE(rd.update_time) = DATE(vi.create_time) or vi.money_flow = "MONEY_OUT" or rd.refund_amount = 0
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
    rd.status as return_status,
    mapping.gia_gach,
    mapping.gia_ban_daily,
    case
        when rd.return_id is not null
        then 0
        else i.shopee_discount
    end AS tro_gia_tu_shopee
  FROM {{ ref('t1_shopee_shop_fee_total') }} AS detail,
  UNNEST(items) AS i
  LEFT JOIN `crypto-arcade-453509-i8`.`google_sheet`.`mapping_brand_sku` AS mapping ON i.model_sku = mapping.ma_sku and detail.brand = mapping.brand
  LEFT JOIN return_detail rd ON detail.order_id = rd.order_id AND i.model_sku = rd.variation_sku and detail.brand = rd.brand and rd.status = 'ACCEPTED'
  LEFT JOIN total_amount ta ON ta.order_id = detail.order_id and ta.brand = detail.brand
  LEFT JOIN {{ ref('t1_shopee_shop_wallet_total') }} vi ON detail.order_id = vi.order_id and detail.brand = vi.brand and vi.transaction_tab_type = 'wallet_order_income'
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
    COALESCE(((sd.discounted_price) / ta.total_tong_tien_san_pham) * ord.days_to_ship, 0) AS day_to_ship
  FROM sale_detail AS sd
  LEFT JOIN {{ ref('t1_shopee_shop_order_detail_total') }} AS ord
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
    CASE 
        -- Case 1: With parentheses, e.g., "Trắng,XS (dưới 50kg)" or "Den,S (50kg-60kg)"
        WHEN REGEXP_CONTAINS(model_name, r',([A-Za-z]+) \([^\)]+\)') 
        THEN REGEXP_EXTRACT(model_name, r',([A-Za-z]+) \([^\)]+\)')
        -- Case 2: With size range or letter, e.g., "Quần Đỏ Đỏ,S/M" or "Xanh navy,M"
        WHEN REGEXP_CONTAINS(model_name, r',([A-Za-z\/]+)$') 
        THEN REGEXP_EXTRACT(model_name, r',([A-Za-z\/]+)$')
        ELSE NULL
    END AS size,
     CASE 
        -- Case 1: With parentheses, e.g., "Trắng,XS (dưới 50kg)"
        WHEN REGEXP_CONTAINS(model_name, r',([A-Za-z]+) \([^\)]+\)') 
        THEN TRIM(SUBSTR(model_name, 1, STRPOS(model_name, ',') - 1))
        -- Case 2: With size range or letter, e.g., "Quần Đỏ Đỏ,S/M"
        WHEN REGEXP_CONTAINS(model_name, r',([A-Za-z\/]+)$') 
        THEN TRIM(SUBSTR(model_name, 1, STRPOS(model_name, ',') - 1))
        -- Case 3: No comma, e.g., "Hồng"
        WHEN NOT REGEXP_CONTAINS(model_name, r',') 
        THEN model_name
        ELSE NULL
    END AS color,
  CASE 
    WHEN model_sku = ""
    THEN item_sku
    ELSE model_sku
  END AS sku_code,
  model_sku,
  quantity_purchased,
  gia_san_pham_goc,
  nguoi_ban_tro_gia,
  discounted_price,
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
COALESCE(tong_tien_san_pham, 0) - COALESCE(so_tien_hoan_tra, 0) - COALESCE(voucher_from_seller, 0) + COALESCE(phi_van_chuyen_nguoi_mua_tra, 0) - COALESCE(phi_van_chuyen_thuc_te, 0) + COALESCE(phi_van_chuyen_tro_gia_tu_shopee, 0) + COALESCE(tro_gia_tu_shopee, 0) - COALESCE(phi_co_dinh, 0) - COALESCE(phi_dich_vu, 0) - COALESCE(phi_thanh_toan, 0) - COALESCE(phi_hoa_hong_tiep_thi_lien_ket, 0) AS doanh_thu_don_hang_uoc_tinh,
COALESCE(shopee_voucher, 0) AS shopee_voucher,
COALESCE(discount_from_coin, 0) AS discount_from_coin,
COALESCE(discount_from_voucher_seller, 0) AS discount_from_voucher_seller,
COALESCE(khuyen_mai_cho_the_tin_dung, 0) AS khuyen_mai_cho_the_tin_dung,
COALESCE(tong_tien_san_pham, 0) - COALESCE(shopee_voucher, 0) - COALESCE(discount_from_coin, 0) - COALESCE(discount_from_voucher_seller, 0) - COALESCE(khuyen_mai_cho_the_tin_dung, 0) AS tong_tien_thanh_toan,
return_status,
COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) AS gia_san_pham_goc_total,
COALESCE(gia_gach, 0) AS gia_gach,
COALESCE(gia_gach, 0) * COALESCE(quantity_purchased, 0) AS tong_gia_gach,
COALESCE(gia_ban_daily, 0) AS gia_ban_daily,
COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0) AS gia_ban_daily_total,
COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) - COALESCE(nguoi_ban_tro_gia, 0) - COALESCE(discount_from_voucher_seller, 0) AS tien_sp_sau_giam_gia,
(COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0)) AS tien_ban_daily_truoc_chiet_khau,
(COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0)) - (COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) - COALESCE(nguoi_ban_tro_gia, 0) - COALESCE(discount_from_voucher_seller, 0)) AS tien_chiet_khau_sp,
CASE
    WHEN LOWER(return_status) = 'accepted' THEN 'Đã hoàn'
    WHEN LOWER(order_status) = 'cancelled' THEN 'Đã hủy'
    WHEN LOWER(order_status) IN ('ready_to_ship', 'processed') THEN 'Đăng đơn'
    WHEN LOWER(order_status) = 'to_confirm_receive' THEN 'Đăng đơn'
    WHEN LOWER(order_status) = 'to_return' THEN 'Đã hoàn'
    WHEN LOWER(order_status) = 'unpaid' THEN 'Đăng đơn'
    WHEN LOWER(order_status) IN ('completed', 'shipped') THEN 'Đã giao thành công'
    ELSE ""
END AS status,
(COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0)) - ((COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0)) - (COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) - COALESCE(nguoi_ban_tro_gia, 0) - COALESCE(discount_from_voucher_seller, 0))) AS doanh_thu

FROM sale_order_detail
--- tổng tiền sản phẩm là lấy (gia_san_pham_goc- chiết khấu người bán) * quantity 