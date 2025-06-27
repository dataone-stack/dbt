-- SELECT 
--     brand,
--     GENERATE_UUID() as ma_giao_dich,
--     'Order' as don_hang_san_pham,
--     order_id,
--     ma_so_thue,
--     ma_yeu_cau_hoan_tien,
--     '-' as ma_san_pham,
--     '-' as ten_san_pham,
--     ngay_dat_hang,
--     ngay_hoan_thanh_thanh_toan,
--     phuong_thuc_thanh_toan,
--     loai_don_hang,
--     Round(SUM(tong_tien_da_thanh_toan)) AS tong_tien_da_thanh_toan,
--     SUM(tong_tien_san_pham) AS tong_tien_san_pham,
--     SUM(so_tien_hoan_lai) AS so_tien_hoan_lai,
--     SUM(phi_van_chuyen_nguoi_mua_tra) AS phi_van_chuyen_nguoi_mua_tra,
--     SUM(phi_van_chuyen_thuc_te) AS phi_van_chuyen_thuc_te,
--     SUM(phi_van_chuyen_tro_gia_tu_shopee) AS phi_van_chuyen_tro_gia_tu_shopee,
--     SUM(phi_tra_hang) AS phi_tra_hang,
--     SUM(phi_van_chuyen_duoc_hoan_tien) AS phi_van_chuyen_duoc_hoan_tien,
--     SUM(phi_tra_hang_cho_nguoi_ban) AS phi_tra_hang_cho_nguoi_ban,
--     SUM(tro_gia_tu_shopee) AS tro_gia_tu_shopee,
--     SUM(ma_giam_gia) AS ma_giam_gia,
--     SUM(nguoi_ban_hoan_xu) AS nguoi_ban_hoan_xu,
--     SUM(phi_co_dinh) AS phi_co_dinh,
--     SUM(phi_dich_vu) AS phi_dich_vu,
--     SUM(phi_thanh_toan) AS phi_thanh_toan,
--     SUM(phi_hoa_hong_tiep_thi_lien_ket) AS phi_hoa_hong_tiep_thi_lien_ket,
--     SUM(phi_dich_vu_piship) AS phi_dich_vu_piship,
--     thue_gtgt,
--     thue_tncn,
--     SUM(buyer_paid_installation_fee) AS buyer_paid_installation_fee,
--     SUM(actual_installition_fee) AS actual_installition_fee,
--     ten_nguoi_mua,
--     SUM(amount_paid_by_buyer) AS amount_paid_by_buyer,
--     5 AS transaction_fee_rate,
--     phuong_thuc_thanh_toan_nguoi_mua,
--     buyer_payment_method_details_1,
--     installment_plan,
--     SUM(phi_van_chuyen_seller_support) AS phi_van_chuyen_seller_support,
--     ten_don_vi_van_chuyen,
--     couruer_name,
--     voucher_code,
--     den_bu_don_mat_hang,
--     SUM(gia_san_pham_sau_khuyen_mai) AS gia_san_pham_sau_khuyen_mai,
--     SUM(shopee_xu) AS shopee_xu,
--     --SUM(shopee_voucher) AS shopee_voucher,
--     0 AS shopee_voucher,
--     SUM(ngan_hang_khuyen_mai_the_tin_dung) AS ngan_hang_khuyen_mai_the_tin_dung,
--     SUM(shopee_khuyen_mai_the_tin_dung) AS shopee_khuyen_mai_the_tin_dung,
--     0 AS gia_ban_daily_total
-- FROM {{ref("t2_shopee_order_lines_tot")}}
-- GROUP BY 
--     order_id, 
--     ngay_dat_hang, 
--     ngay_hoan_thanh_thanh_toan, 
--     phuong_thuc_thanh_toan, 
--     loai_don_hang, 
--     ma_so_thue, 
--     ma_yeu_cau_hoan_tien, 
--     thue_gtgt, 
--     thue_tncn, 
--     ten_nguoi_mua, 
--     phuong_thuc_thanh_toan_nguoi_mua, 
--     transaction_fee_rate, 
--     buyer_payment_method_details_1, 
--     installment_plan, 
--     ten_don_vi_van_chuyen, 
--     couruer_name, 
--     voucher_code, 
--     den_bu_don_mat_hang, 
--     brand
WITH 
-- Tính tổng giá daily và doanh thu kế toán theo order
order_product_summary AS (
  SELECT 
    f.order_id,
    f.brand,
    SUM(COALESCE(mapping.gia_ban_daily, 0) * COALESCE(i.quantity_purchased, 0)) AS gia_ban_daily_total,
    SUM(
      (COALESCE(mapping.gia_ban_daily, 0) * COALESCE(i.quantity_purchased, 0)) - 
      ((COALESCE(mapping.gia_ban_daily, 0) * COALESCE(i.quantity_purchased, 0)) - 
       (COALESCE(i.original_price, 0) - COALESCE(i.seller_discount, 0) - COALESCE(i.discount_from_voucher_seller, 0)))
    ) AS doanh_thu_ke_toan
  FROM {{ ref('t1_shopee_shop_fee_total') }} f,
  UNNEST(items) AS i
  LEFT JOIN {{ ref('t1_bang_gia_san_pham') }} AS mapping ON 
    CASE 
      WHEN i.model_sku = "" THEN i.item_sku
      ELSE i.model_sku
    END = mapping.ma_sku AND f.brand = mapping.brand
  GROUP BY f.order_id, f.brand
)

SELECT 
    f.brand,
    GENERATE_UUID() as ma_giao_dich,
    'ORDER' as don_hang_san_pham,
    f.order_id,
    f.tax_code as ma_so_thue,
    vi.refund_sn as ma_yeu_cau_hoan_tien,
    '' as ma_san_pham,
    '' as ten_san_pham,
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) as ngay_dat_hang,
    DATETIME_ADD(vi.create_time, INTERVAL 7 HOUR) as ngay_tien_ve_vi,
    "Ví ShopeePay" as phuong_thuc_thanh_toan,
    'Đơn thường' as loai_don_hang,
    
    -- Lấy thẳng các phí từ bảng fee_total
    vi.amount as tong_tien_da_thanh_toan,
    -- f.buyer_paid_shipping_fee as phi_van_chuyen_nguoi_mua_tra,
    -- f.actual_shipping_fee * -1 as phi_van_chuyen_thuc_te,
    -- f.shopee_shipping_rebate as phi_van_chuyen_tro_gia_tu_shopee,
    -- f.shopee_discount as tro_gia_tu_shopee,
    f.voucher_from_seller * -1 as ma_giam_gia,
    f.commission_fee * -1 as phi_co_dinh,
    f.service_fee * -1 as phi_dich_vu,
    f.seller_transaction_fee * -1 as phi_thanh_toan,
    f.order_ams_commission_fee * -1 as phi_hoa_hong_tiep_thi_lien_ket,
    f.credit_card_promotion as ngan_hang_khuyen_mai_the_tin_dung,
    
    -- Lấy từ wallet
    vi.amount as wallet_amount,
    
    -- Lấy từ order summary
    ops.gia_ban_daily_total,
    ops.doanh_thu_ke_toan,
    
    -- Thông tin order
    -- ord.buyer_user_name as ten_nguoi_mua,
    -- ord.payment_method as hinh_thuc_thanh_toan,
    -- ord.shipping_carrier as ten_don_vi_van_chuyen,
    -- ord.checkout_shipping_carrier as couruer_name,
    -- f.instalment_plan as installment_plan,
    -- f.seller_voucher_code as voucher_code

FROM {{ ref('t1_shopee_shop_fee_total') }} f
LEFT JOIN {{ ref('t1_shopee_shop_wallet_total') }} vi 
    ON f.order_id = vi.order_id 
    AND f.brand = vi.brand 
    AND vi.transaction_tab_type = 'wallet_order_income'
LEFT JOIN {{ ref('t1_shopee_shop_order_detail_total') }} ord 
    ON f.order_id = ord.order_id 
    AND f.brand = ord.brand
LEFT JOIN order_product_summary ops 
    ON f.order_id = ops.order_id 
    AND f.brand = ops.brand

