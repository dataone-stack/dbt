
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

-- Tính tổng giá daily và doanh thu kế toán theo order
order_product_summary AS (
  SELECT 
    f.order_id,
    f.brand,
    SUM(COALESCE(mapping.gia_ban_daily, 0) * COALESCE(i.quantity_purchased, 0)) AS gia_ban_daily_total,
    SUM(
       (COALESCE(i.original_price, 0) - COALESCE(i.seller_discount, 0) - COALESCE(i.discount_from_voucher_seller, 0))
    ) AS doanh_thu_ke_toan,
     CASE
      WHEN sum(rd.refund_amount) = 0
      THEN sum(rd.so_tien_hoan_tra) * -1
      ELSE 0
    END AS so_tien_hoan_tra,
    0 as nguoi_ban_hoan_xu,
    sum(shopee_discount) as tro_gia_shopee,
    sum(COALESCE(i.original_price, 0)) as gia_goc,
    sum(COALESCE(i.seller_discount, 0))*-1 as seller_tro_gia,
  FROM {{ ref('t1_shopee_shop_fee_total') }} f,
  UNNEST(items) AS i
  LEFT JOIN {{ ref('t1_bang_gia_san_pham') }} AS mapping ON 
    CASE 
      WHEN i.model_sku = "" THEN i.item_sku
      ELSE i.model_sku  
    END = mapping.ma_sku AND f.brand = mapping.brand
  LEFT JOIN return_detail rd ON f.order_id = rd.order_id AND i.model_sku = rd.variation_sku and f.brand = rd.brand and rd.status = 'ACCEPTED'
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
    (ops.gia_ban_daily_total - ops.doanh_thu_ke_toan) as tien_chiet_khau_sp,
    (ops.gia_ban_daily_total -(ops.gia_goc + (ops.seller_tro_gia  + ops.so_tien_hoan_tra -ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu))) as tien_chiet_khau_sp_shopee,
    ops.gia_goc,
    ops.seller_tro_gia,
    ops.so_tien_hoan_tra,
    ops.tro_gia_shopee,
    (ops.gia_goc + (ops.seller_tro_gia  + ops.so_tien_hoan_tra -ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu)) as doanh_thu_shopee,
    (f.commission_fee *-1) + (f.service_fee *-1) + (f.seller_transaction_fee *-1) + (f.order_ams_commission_fee *-1) AS phu_phi

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
