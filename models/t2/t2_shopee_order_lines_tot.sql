-WITH return_detail AS (
  SELECT 
    order_id,
    return_id,
    brand,
    update_time,
    i.variation_sku, 
    i.item_price * i.amount AS so_tien_hoan_tra,
    status,
    refund_amount,
    return_seller_due_date,
    amount_before_discount
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_order_retrurn_total`,
  UNNEST(item) AS i
),

total_amount AS (
  SELECT 
    order_id,
    brand,
    SUM(i.discounted_price) AS total_tong_tien_san_pham,
    instalment_plan,
    seller_voucher_code,
    seller_shipping_discount,
    credit_card_promotion
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_fee_total`,   
  UNNEST(items) AS i
  GROUP BY order_id,brand,instalment_plan,seller_voucher_code,seller_shipping_discount,credit_card_promotion
),

sale_detail AS (
  SELECT 
    detail.order_id,
    detail.buyer_user_name AS ten_nguoi_mua,
    detail.brand,
    i.model_sku,
    i.item_name,
    i.model_name,
    i.item_id,
    i.quantity_purchased,
    (i.original_price / i.quantity_purchased) AS gia_san_pham_goc,
    i.discounted_price,
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
    rd.amount_before_discount,
    vi.refund_sn,
    
    case
        when rd.return_id is not null
        then 0
        else i.shopee_discount
    end AS tro_gia_tu_shopee
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_fee_total` AS detail,
  UNNEST(items) AS i
  LEFT JOIN return_detail rd ON detail.order_id = rd.order_id AND i.model_sku = rd.variation_sku and detail.brand = rd.brand and rd.status = 'ACCEPTED'
  LEFT JOIN total_amount ta ON ta.order_id = detail.order_id and ta.brand = detail.brand
  LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_wallet_total` vi ON detail.order_id = vi.order_id and detail.brand = vi.brand and vi.transaction_tab_type = 'wallet_order_income'
  
),

sale_order_detail AS (
  SELECT
    sd.*,
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) AS create_time,
    ord.order_status,
    ord.payment_method AS hinh_thuc_thanh_toan,
    ord.ship_by_date AS ngay_ship,
    ord.buyer_cancel_reason AS ly_do_huy_don,
    COALESCE(((sd.discounted_price) / ta.total_tong_tien_san_pham) * ord.days_to_ship, 0) AS day_to_ship,
    COALESCE(((sd.discounted_price) / ta.total_tong_tien_san_pham) * ord.total_amount, 0) AS doanh_thu_don_hang,
    ord.checkout_shipping_carrier AS courier_name,
    ord.shipping_carrier  AS ten_don_vi_van_chuyen,
    ta.instalment_plan,
    ta.seller_voucher_code,
    ta.seller_shipping_discount,
    ta.credit_card_promotion,
  FROM sale_detail AS sd
  LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_order_detail_total` AS ord
    ON sd.order_id = ord.order_id and sd.brand = ord.brand
  LEFT JOIN total_amount ta ON ta.order_id = sd.order_id and ta.brand = sd.brand
)

SELECT 
    brand as brand,
    ''as ma_giao_dich,
    'SKU' as don_hang_san_pham,
    order_id,
    ''as ma_so_thue,
    refund_sn as ma_yeu_cau_hoan_tien, -- chưa lấy được
    item_id as ma_san_pham,
    item_name as ten_san_pham,
    create_time as ngay_dat_hang,
    DATETIME_ADD(ngay_tien_ve_vi, INTERVAL 7 HOUR) as ngay_hoan_thanh_thanh_toan,
   "Ví ShopeePay" as phuong_thuc_thanh_toan,
    'Đơn thường' as loai_don_hang,
    tong_tien_san_pham - phi_co_dinh - phi_dich_vu - phi_thanh_toan  - phi_hoa_hong_tiep_thi_lien_ket - (phi_van_chuyen_thuc_te - phi_van_chuyen_tro_gia_tu_shopee) - so_tien_hoan_tra - tro_gia_tu_shopee + ma_giam_gia - phi_van_chuyen_nguoi_mua_tra as tong_tien_da_thanh_toan,
    tong_tien_san_pham,
    so_tien_hoan_tra * -1 as so_tien_hoan_lai,
    phi_van_chuyen_nguoi_mua_tra as phi_van_chuyen_nguoi_mua_tra,
    phi_van_chuyen_thuc_te * -1 as phi_van_chuyen_thuc_te,
    phi_van_chuyen_tro_gia_tu_shopee as phi_van_chuyen_tro_gia_tu_shopee,
    0 as phi_tra_hang,
    0 as phi_van_chuyen_duoc_hoan_tien,
    0 as phi_tra_hang_cho_nguoi_ban,
    tro_gia_tu_shopee as tro_gia_tu_shopee,
    voucher_from_seller * -1 as ma_giam_gia,
    0 as nguoi_ban_hoan_xu,
    phi_co_dinh * -1 as phi_co_dinh,
    phi_dich_vu * -1 as phi_dich_vu,
    phi_thanh_toan * -1 as phi_thanh_toan,
    phi_hoa_hong_tiep_thi_lien_ket * -1 as phi_hoa_hong_tiep_thi_lien_ket,
    0 as phi_dich_vu_piship,
    0 as thue_gtgt,
    0 as thue_tncn,
    0 as buyer_paid_installation_fee,
    0 as actual_installition_fee,
    ten_nguoi_mua,
    doanh_thu_don_hang as amount_paid_by_buyer,
    0 as transaction_fee_rate,
    hinh_thuc_thanh_toan as phuong_thuc_thanh_toan_nguoi_mua,
    ''as buyer_payment_method_details_1,
    instalment_plan as installment_plan,
    discount_from_voucher_seller as phi_van_chuyen_seller_support, -- kiểm tra thêm ý phí vận chuyển seller support
    ten_don_vi_van_chuyen,
    courier_name as couruer_name,
    seller_voucher_code as voucher_code,
    0 as den_bu_don_mat_hang, --- chưa biết
    amount_before_discount * -1 as gia_san_pham_sau_khuyen_mai,
    discount_from_coin as shopee_xu,
    shopee_voucher,
    credit_card_promotion as ngan_hang_khuyen_mai_the_tin_dung,
    0 as shopee_khuyen_mai_the_tin_dung,
FROM sale_order_detail