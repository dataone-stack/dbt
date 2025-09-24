WITH return_detail AS (
  SELECT 
    order_id,
    return_id,
    b.brand,
    b.brand_lv1,
    update_time,
    i.variation_sku, 
    i.item_price * i.amount AS so_tien_hoan_tra,
    status,
    refund_amount,
    return_seller_due_date
  FROM {{ ref('t1_shopee_shop_order_retrurn_total') }},
  UNNEST(item) AS i
  left join `dtm.t1_bang_gia_san_pham` as b on trim(i.variation_sku) = trim(b.ma_sku)
) 
,

total_amount AS (
  SELECT 
    a.order_id,
    b.brand,
    b.brand_lv1,
    SUM(i.discounted_price) AS total_tong_tien_san_pham
  FROM {{ ref('t1_shopee_shop_fee_total') }} a,   
  UNNEST(items) AS i
  left join `dtm.t1_bang_gia_san_pham` as b on 
  trim(CASE 
        WHEN i.model_sku = ""
        THEN i.item_sku
    ELSE i.model_sku
  END) = trim (b.ma_sku)
  GROUP BY a.order_id,b.brand, b.brand_lv1
)

,shopee_fee_total as (
  select
    detail.shop,
    detail.order_id,
    detail.buyer_user_name ,
    mapping.brand,
    mapping.brand_lv1,
    detail.company,
    detail.buyer_paid_shipping_fee ,
    detail.commission_fee ,
    detail.service_fee ,
    detail.seller_transaction_fee ,
    detail.actual_shipping_fee ,
    detail.shopee_shipping_rebate ,
    detail.credit_card_promotion ,
    detail.order_ams_commission_fee ,
    detail.voucher_from_seller ,
    mapping.gia_ban_daily,
    i.model_sku,
    i.item_sku,
     CASE 
        WHEN i.model_sku = ""
        THEN i.item_sku
    ELSE i.model_sku
  END as sku,
    i.item_name,
    i.model_name,
    i.quantity_purchased,
    i.original_price,

    i.seller_discount,
    i.discounted_price,
     i.discount_from_voucher_shopee ,
    i.discount_from_coin,
    i.discount_from_voucher_seller,
    i.shopee_discount
  FROM {{ ref('t1_shopee_shop_fee_total') }} AS detail,
  UNNEST(items) AS i
  LEFT JOIN {{ ref('t1_bang_gia_san_pham') }} AS mapping ON 
  trim(CASE 
        WHEN i.model_sku = ""
        THEN i.item_sku
    ELSE i.model_sku
  END) = trim(mapping.ma_sku)
)

,shopee_order_detail as (
  select 
    ord.order_id,
    ord.create_time,
    ord.order_status,
    ord.payment_method ,
    ord.shipping_carrier ,
    ord.ship_by_date ,
    CASE 
        WHEN i.model_sku = ""
        THEN i.item_sku
    ELSE i.model_sku
  END as sku,
    ord.buyer_cancel_reason,
    mapping.brand,
    mapping.brand_lv1,
    ord.days_to_ship
  from {{ ref('t1_shopee_shop_order_detail_total') }} as ord, 
  unnest (item_list) as i
  LEFT JOIN {{ ref('t1_bang_gia_san_pham') }} AS mapping ON 
  trim(CASE 
        WHEN i.model_sku = ""
        THEN i.item_sku
    ELSE i.model_sku
  END) = trim(mapping.ma_sku)
)

,sale_detail AS (
  SELECT
    detail.shop,
    detail.order_id,
    detail.buyer_user_name AS ten_nguoi_mua,
    detail.brand,
    detail.brand_lv1,
    detail.company,
    detail.model_sku,
    detail.item_sku,
    detail.sku,
    detail.item_name,
    detail.model_name,
    detail.quantity_purchased,
    safe_divide(detail.original_price,detail.quantity_purchased) AS gia_san_pham_goc,
    detail.seller_discount AS nguoi_ban_tro_gia,
    detail.discounted_price,
    --(i.original_price/i.quantity_purchased) as test_doanh_thu,
    rd.update_time AS ngay_return,
    vi.create_time AS ngay_tien_ve_vi,
    CASE
      WHEN DATE(rd.update_time) = DATE(vi.create_time) or vi.money_flow = "MONEY_OUT" or rd.refund_amount = 0
      THEN rd.so_tien_hoan_tra
      ELSE 0
    END AS so_tien_hoan_tra,
    (detail.discounted_price) AS tong_tien_san_pham,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.buyer_paid_shipping_fee AS phi_van_chuyen_nguoi_mua_tra,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.commission_fee AS phi_co_dinh,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.service_fee AS phi_dich_vu,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.seller_transaction_fee  AS phi_thanh_toan,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.actual_shipping_fee  AS phi_van_chuyen_thuc_te,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.shopee_shipping_rebate  AS phi_van_chuyen_tro_gia_tu_shopee,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.credit_card_promotion  AS khuyen_mai_cho_the_tin_dung,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.order_ams_commission_fee  AS phi_hoa_hong_tiep_thi_lien_ket,
    safe_divide(detail.discounted_price , ta.total_tong_tien_san_pham) * detail.voucher_from_seller  AS voucher_from_seller,
    detail.discount_from_voucher_shopee AS shopee_voucher,
    detail.discount_from_coin,
    detail.discount_from_voucher_seller,
    rd.status as return_status,
    detail.gia_ban_daily,
    case
        when rd.return_id is not null
        then 0
        else detail.shopee_discount
    end AS tro_gia_tu_shopee
  FROM shopee_fee_total AS detail
  LEFT JOIN return_detail rd ON detail.order_id = rd.order_id AND trim(detail.sku) = trim(rd.variation_sku) and detail.brand = rd.brand and rd.status = 'ACCEPTED'
  LEFT JOIN total_amount ta ON ta.order_id = detail.order_id and ta.brand = detail.brand
  LEFT JOIN {{ ref('t1_shopee_shop_wallet_total') }} vi ON detail.order_id = vi.order_id and vi.transaction_tab_type = 'wallet_order_income'
)
, sale_order_detail AS (
  SELECT
    sd.*,
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) AS create_time,
    ord.order_status,
    ord.payment_method AS hinh_thuc_thanh_toan,
    ord.shipping_carrier AS ten_don_vi_van_chuyen,
    ord.ship_by_date AS ngay_ship,
    ord.buyer_cancel_reason AS ly_do_huy_don,
    safe_divide ( sd.discounted_price , ta.total_tong_tien_san_pham) * ord.days_to_ship  AS day_to_ship
  FROM sale_detail AS sd
  LEFT JOIN shopee_order_detail AS ord
    ON sd.order_id = ord.order_id and sd.brand = ord.brand and trim(sd.sku) = trim(ord.sku)
  LEFT JOIN total_amount ta ON ta.order_id = sd.order_id and ta.brand = sd.brand
)

SELECT
  --test_doanh_thu,
  shop,
  create_time as ngay_tao_don,
  ten_nguoi_mua,
  brand,
  brand_lv1,
  company,
  ngay_return,
  ngay_tien_ve_vi,
  hinh_thuc_thanh_toan,
  ten_don_vi_van_chuyen,
  ngay_ship,
  day_to_ship,
  ly_do_huy_don,
  order_status,
  order_id as ma_don_hang,
  item_name as ten_san_pham,
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
  quantity_purchased as so_luong,
  gia_san_pham_goc,
  nguoi_ban_tro_gia as seller_tro_gia,
  discounted_price,
  tong_tien_san_pham,
  so_tien_hoan_tra,

  abs(phi_van_chuyen_nguoi_mua_tra) as phi_ship,
  abs(phi_van_chuyen_thuc_te) * -1 as phi_van_chuyen_thuc_te,
  abs(phi_van_chuyen_tro_gia_tu_shopee) as phi_van_chuyen_tro_gia_tu_san,
  abs(phi_co_dinh) * -1 as phi_co_dinh,
  abs(phi_dich_vu) * -1 as phi_dich_vu,
  abs(phi_thanh_toan) * -1 as phi_thanh_toan,
  abs(phi_hoa_hong_tiep_thi_lien_ket)*-1 as phi_hoa_hong_tiep_thi_lien_ket,
  abs(tro_gia_tu_shopee) as san_tro_gia,
  abs(voucher_from_seller) * -1 as voucher_from_seller,
  0 as phi_hoa_hong_shop,
  0 as phi_hoa_hong_quang_cao_cua_hang,
  0 as phi_xtra,

COALESCE(tong_tien_san_pham, 0) - COALESCE(so_tien_hoan_tra, 0) - COALESCE(voucher_from_seller, 0) + COALESCE(phi_van_chuyen_nguoi_mua_tra, 0) - COALESCE(phi_van_chuyen_thuc_te, 0) + COALESCE(phi_van_chuyen_tro_gia_tu_shopee, 0) + COALESCE(tro_gia_tu_shopee, 0) - COALESCE(phi_co_dinh, 0) - COALESCE(phi_dich_vu, 0) - COALESCE(phi_thanh_toan, 0) - COALESCE(phi_hoa_hong_tiep_thi_lien_ket, 0) AS doanh_thu_don_hang_uoc_tinh,

COALESCE(shopee_voucher, 0) AS giam_gia_san,
COALESCE(discount_from_coin, 0) AS discount_from_coin,
COALESCE(discount_from_voucher_seller, 0) AS giam_gia_seller,
COALESCE(khuyen_mai_cho_the_tin_dung, 0) AS khuyen_mai_cho_the_tin_dung,
COALESCE(tong_tien_san_pham, 0) - COALESCE(shopee_voucher, 0) - COALESCE(discount_from_coin, 0) - COALESCE(discount_from_voucher_seller, 0) - COALESCE(khuyen_mai_cho_the_tin_dung, 0) - phi_van_chuyen_nguoi_mua_tra AS tien_khach_hang_thanh_toan,
return_status,
COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) AS gia_san_pham_goc_total,
COALESCE(gia_ban_daily, 0) AS gia_ban_daily,
COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0) AS gia_ban_daily_total,
COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) - COALESCE(nguoi_ban_tro_gia, 0) - COALESCE(discount_from_voucher_seller, 0) AS tien_sp_sau_tro_gia,
(COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0)) - (COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) - COALESCE(nguoi_ban_tro_gia, 0) - COALESCE(discount_from_voucher_seller, 0)) AS tien_chiet_khau_sp,
CASE
    WHEN LOWER(return_status) = 'accepted' THEN 'Đã hoàn'
    WHEN LOWER(order_status) IN ('cancelled', 'in_cancel') THEN 'Đã hủy'
    WHEN LOWER(order_status) IN ('ready_to_ship', 'processed') THEN 'Đăng đơn'
    WHEN LOWER(order_status) = 'to_confirm_receive' THEN 'Đăng đơn'
    WHEN LOWER(order_status) = 'to_return' THEN 'Đã hoàn'
    WHEN LOWER(order_status) = 'unpaid' THEN 'Đăng đơn'
    WHEN LOWER(order_status) IN ('completed', 'shipped') THEN 'Đã giao thành công'
    ELSE ""
END AS status,
(COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0)) - ((COALESCE(gia_ban_daily, 0) * COALESCE(quantity_purchased, 0)) - (COALESCE(gia_san_pham_goc, 0) * COALESCE(quantity_purchased, 0) - COALESCE(nguoi_ban_tro_gia, 0) - COALESCE(discount_from_voucher_seller, 0))) AS doanh_thu_ke_toan,

COALESCE(voucher_from_seller, 0) - COALESCE(phi_van_chuyen_nguoi_mua_tra, 0) + COALESCE(phi_van_chuyen_thuc_te, 0) - COALESCE(phi_van_chuyen_tro_gia_tu_shopee, 0) - COALESCE(tro_gia_tu_shopee, 0) + COALESCE(phi_co_dinh, 0) + COALESCE(phi_dich_vu, 0) + COALESCE(phi_thanh_toan, 0) + COALESCE(phi_hoa_hong_tiep_thi_lien_ket, 0) as tong_phi_san
FROM sale_order_detail 