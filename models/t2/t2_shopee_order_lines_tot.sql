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
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_order_retrurn_total`,
  UNNEST(item) AS i
),


total_amount AS (
  SELECT 
    a.order_id,
    b.brand,
    b.brand_lv1,
    SUM(i.discounted_price) AS total_tong_tien_san_pham
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_fee_total` a,   
  UNNEST(items) AS i
  left join `dtm.t1_bang_gia_san_pham` as b on 
  trim(CASE 
        WHEN i.model_sku = ""
        THEN i.item_sku
    ELSE i.model_sku
  END) = trim (b.ma_sku)
  GROUP BY a.order_id,b.brand, b.brand_lv1
)
,
order_detail as (
    select order_id,
     CASE 
        WHEN i.model_sku = "" THEN i.item_sku
        ELSE i.model_sku  
    END as model_sku,

    i.promotion_type,
    brand,
    shop,
    company
    from `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_order_detail_total`
    cross join unnest (item_list) as i
),

-- Tính tổng giá daily và doanh thu kế toán theo order
order_product_summary AS (
  SELECT 
    f.order_id,
    f.brand,
    CASE 
      WHEN i.model_sku = "" THEN i.item_sku
      ELSE i.model_sku  
    END as sku_code,
    i.item_name,
    i.quantity_purchased,
    CASE
        when ord.promotion_type = 'add_on_free_gift_sub'
        then 0
        else COALESCE(mapping.gia_ban_daily, 0) * COALESCE(i.quantity_purchased, 0)
    end as gia_ban_daily_total,
     CASE
        when ord.promotion_type = 'add_on_free_gift_sub'
        then 0
        else  
        COALESCE(i.original_price, 0) - COALESCE(i.seller_discount, 0) - COALESCE(i.discount_from_voucher_seller, 0)
    end as doanh_thu_ke_toan,
    safe_divide(i.original_price,i.quantity_purchased) AS gia_san_pham_goc,
    safe_divide(i.original_price,i.quantity_purchased) * i.quantity_purchased as gia_san_pham_goc_total,
     CASE
      WHEN rd.refund_amount = 0
      THEN rd.so_tien_hoan_tra * -1
      ELSE 0
    END AS so_tien_hoan_tra,
    CASE
        when ord.promotion_type = 'add_on_free_gift_sub'
        then "Quà Tặng"
    end as promotion_type,
    0 as nguoi_ban_hoan_xu,
    shopee_discount as tro_gia_shopee,
    COALESCE(i.original_price, 0) as gia_goc,
    COALESCE(i.seller_discount, 0)*-1 as seller_tro_gia,
    COALESCE(cost_price.cost_price, 0) * i.quantity_purchased as gia_von,
    COALESCE(i.discounted_price,0) as discounted_price,

  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_fee_total` f,
  UNNEST(items) AS i
  LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_bang_gia_san_pham` AS mapping ON 
    CASE 
      WHEN i.model_sku = "" THEN i.item_sku
      ELSE i.model_sku  
    END = mapping.ma_sku AND f.brand = mapping.brand
  LEFT JOIN return_detail rd ON f.order_id = rd.order_id AND i.model_sku = rd.variation_sku and f.brand = rd.brand and rd.status = 'ACCEPTED'

  LEFT JOIN order_detail ord on f.order_id = ord.order_id  and
    CASE 
        WHEN i.model_sku = "" THEN i.item_sku
        ELSE i.model_sku  
    END = ord.model_sku
    and f.brand = ord.brand
  left join `google_sheet.bang_gia_von` as cost_price on 
    CASE 
        WHEN i.model_sku = "" THEN i.item_sku
        ELSE i.model_sku  
    END = cost_price.product_sku

), a as(

SELECT 
    f.brand,
    f.company,
    DATETIME_ADD(ord.ship_by_date, INTERVAL 7 HOUR) as ngay_ship,
   
    case
        WHEN LOWER(ord.order_status) IN ('cancelled', 'in_cancel') THEN 'Đã hủy'
        WHEN LOWER(ord.order_status) IN ('ready_to_ship', 'processed') THEN 'Đăng đơn'
        WHEN LOWER(ord.order_status) = 'to_confirm_receive' THEN 'Đăng đơn'
        WHEN LOWER(ord.order_status) = 'to_return' THEN 'Đã hoàn'
        WHEN LOWER(ord.order_status) = 'unpaid' THEN 'Đăng đơn'
        WHEN LOWER(ord.order_status) IN ('completed', 'shipped') THEN 'Đã giao thành công'
        ELSE ""
    end as status,
    
    GENERATE_UUID() as ma_giao_dich,
    'ORDER' as don_hang_san_pham,

    f.order_id,
    f.tax_code as ma_so_thue,
    vi.refund_sn as ma_yeu_cau_hoan_tien,
    ops.sku_code as ma_san_pham,
    ops.item_name as ten_san_pham,
    ops.quantity_purchased as so_luong,
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) as ngay_dat_hang,
    DATETIME_ADD(vi.create_time, INTERVAL 7 HOUR) as ngay_tien_ve_vi,
    "Ví ShopeePay" as phuong_thuc_thanh_toan,
    'Đơn thường' as loai_don_hang,
    
    -- Lấy thẳng các phí từ bảng fee_total
    -- vi.amount as tong_tien_da_thanh_toan,
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
    ops.gia_san_pham_goc,
    ops.gia_san_pham_goc_total,
    ops.doanh_thu_ke_toan,
    ops.doanh_thu_ke_toan as doanh_thu_ke_toan_v2,
    (ops.gia_ban_daily_total - ops.doanh_thu_ke_toan) as tien_chiet_khau_sp,
    (ops.gia_ban_daily_total -(ops.gia_goc + (ops.seller_tro_gia  + ops.so_tien_hoan_tra -ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu))) as tien_chiet_khau_sp_shopee,
    ops.gia_goc,
    ops.seller_tro_gia,
    ops.so_tien_hoan_tra,
    ops.tro_gia_shopee,
    (ops.gia_goc + (ops.seller_tro_gia  + ops.so_tien_hoan_tra -ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu)) as doanh_thu_shopee,
    (safe_divide(ops.discounted_price , ta.total_tong_tien_san_pham) * f.commission_fee *-1) + (safe_divide(ops.discounted_price , ta.total_tong_tien_san_pham) * f.service_fee *-1) + (safe_divide(ops.discounted_price , ta.total_tong_tien_san_pham) * f.seller_transaction_fee *-1) + (safe_divide(ops.discounted_price , ta.total_tong_tien_san_pham) * f.order_ams_commission_fee) AS phu_phi,
    ops.gia_von,
    ops.promotion_type,
    --- chia tỷ lệ
    safe_divide(ops.discounted_price , ta.total_tong_tien_san_pham) * buyer_paid_shipping_fee as phi_van_chuyen_thuc_te,
    safe_divide(ops.discounted_price , ta.total_tong_tien_san_pham) *vi.amount as tong_tien_da_thanh_toan,
FROM `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_fee_total` f
LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_wallet_total` vi 
    ON f.order_id = vi.order_id 
    AND f.brand = vi.brand
    AND vi.transaction_tab_type = 'wallet_order_income'
LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_shopee_shop_order_detail_total` ord 
    ON f.order_id = ord.order_id 
    AND f.brand = ord.brand
LEFT JOIN order_product_summary ops 
    ON f.order_id = ops.order_id 
    AND f.brand = ops.brand
left join total_amount ta ON ta.order_id = f.order_id and ta.brand = f.brand
)

select * from a --where order_id ="25092390SA8DPV"



-- WITH return_detail AS (
--   SELECT 
--     order_id,
--     return_id,
--     brand,
--     update_time,
--     i.variation_sku, 
--     i.item_price * i.amount AS so_tien_hoan_tra,
--     status,
--     refund_amount,
--     return_seller_due_date
--   FROM {{ ref('t1_shopee_shop_order_retrurn_total') }},
--   UNNEST(item) AS i
-- ),
-- order_detail as (
--     select order_id,
--      CASE 
--         WHEN i.model_sku = "" THEN i.item_sku
--         ELSE i.model_sku  
--     END as model_sku,

--     i.promotion_type,
--     brand,
--     shop,
--     company
--     from {{ref("t1_shopee_shop_order_detail_total")}}
--     cross join unnest (item_list) as i
-- ),

-- -- Tính tổng giá daily và doanh thu kế toán theo order
-- order_product_summary AS (
--   SELECT 
--     f.order_id,
--     f.brand,
--     CASE 
--       WHEN i.model_sku = "" THEN i.item_sku
--       ELSE i.model_sku  
--     END as sku_code,
--     i.item_name,
--     i.quantity_purchased,
--     CASE
--         when ord.promotion_type = 'add_on_free_gift_sub'
--         then 0
--         else COALESCE(mapping.gia_ban_daily, 0) * COALESCE(i.quantity_purchased, 0)
--     end as gia_ban_daily_total,
--      CASE
--         when ord.promotion_type = 'add_on_free_gift_sub'
--         then 0
--         else  
--         COALESCE(i.original_price, 0) - COALESCE(i.seller_discount, 0) - COALESCE(i.discount_from_voucher_seller, 0)
--     end as doanh_thu_ke_toan,
--     safe_divide(i.original_price,i.quantity_purchased) AS gia_san_pham_goc,
--     safe_divide(i.original_price,i.quantity_purchased) * i.quantity_purchased as gia_san_pham_goc_total,
--      CASE
--       WHEN rd.refund_amount = 0
--       THEN rd.so_tien_hoan_tra * -1
--       ELSE 0
--     END AS so_tien_hoan_tra,

--     0 as nguoi_ban_hoan_xu,
--     shopee_discount as tro_gia_shopee,
--     COALESCE(i.original_price, 0) as gia_goc,
--     COALESCE(i.seller_discount, 0)*-1 as seller_tro_gia,
--     COALESCE(cost_price.cost_price, 0) * i.quantity_purchased as gia_von
--   FROM {{ ref('t1_shopee_shop_fee_total') }} f,
--   UNNEST(items) AS i
--   LEFT JOIN {{ ref('t1_bang_gia_san_pham') }} AS mapping ON 
--     CASE 
--       WHEN i.model_sku = "" THEN i.item_sku
--       ELSE i.model_sku  
--     END = mapping.ma_sku AND f.brand = mapping.brand
--   LEFT JOIN return_detail rd ON f.order_id = rd.order_id AND i.model_sku = rd.variation_sku and f.brand = rd.brand and rd.status = 'ACCEPTED'

--   LEFT JOIN order_detail ord on f.order_id = ord.order_id  and
--     CASE 
--         WHEN i.model_sku = "" THEN i.item_sku
--         ELSE i.model_sku  
--     END = ord.model_sku
--     and f.brand = ord.brand
--   left join `google_sheet.bang_gia_von` as cost_price on i.model_sku = cost_price.product_sku


-- )

-- SELECT 
--     f.brand,
--     f.company,
--     DATETIME_ADD(ord.ship_by_date, INTERVAL 7 HOUR) as ngay_ship,
   
--     case
--         WHEN LOWER(ord.order_status) IN ('cancelled', 'in_cancel') THEN 'Đã hủy'
--         WHEN LOWER(ord.order_status) IN ('ready_to_ship', 'processed') THEN 'Đăng đơn'
--         WHEN LOWER(ord.order_status) = 'to_confirm_receive' THEN 'Đăng đơn'
--         WHEN LOWER(ord.order_status) = 'to_return' THEN 'Đã hoàn'
--         WHEN LOWER(ord.order_status) = 'unpaid' THEN 'Đăng đơn'
--         WHEN LOWER(ord.order_status) IN ('completed', 'shipped') THEN 'Đã giao thành công'
--         ELSE ""
--     end as status,

--     GENERATE_UUID() as ma_giao_dich,
--     'ORDER' as don_hang_san_pham,

--     f.order_id,
--     f.tax_code as ma_so_thue,
--     vi.refund_sn as ma_yeu_cau_hoan_tien,
--     ops.sku_code as ma_san_pham,
--     ops.item_name as ten_san_pham,
--     ops.quantity_purchased as so_luong,
--     DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) as ngay_dat_hang,
--     DATETIME_ADD(vi.create_time, INTERVAL 7 HOUR) as ngay_tien_ve_vi,
--     "Ví ShopeePay" as phuong_thuc_thanh_toan,
--     'Đơn thường' as loai_don_hang,
    
--     -- Lấy thẳng các phí từ bảng fee_total
--     vi.amount as tong_tien_da_thanh_toan,
--     f.voucher_from_seller * -1 as ma_giam_gia,
--     f.commission_fee * -1 as phi_co_dinh,
--     f.service_fee * -1 as phi_dich_vu,
--     f.seller_transaction_fee * -1 as phi_thanh_toan,
--     f.order_ams_commission_fee * -1 as phi_hoa_hong_tiep_thi_lien_ket,
--     f.credit_card_promotion as ngan_hang_khuyen_mai_the_tin_dung,
    
--     -- Lấy từ wallet
--     vi.amount as wallet_amount,
    
--     -- Lấy từ order summary
--     ops.gia_ban_daily_total,
--     ops.gia_san_pham_goc,
--     ops.gia_san_pham_goc_total,
--     ops.doanh_thu_ke_toan,
--     ops.doanh_thu_ke_toan as doanh_thu_ke_toan_v2,
--     (ops.gia_ban_daily_total - ops.doanh_thu_ke_toan) as tien_chiet_khau_sp,
--     (ops.gia_ban_daily_total -(ops.gia_goc + (ops.seller_tro_gia  + ops.so_tien_hoan_tra -ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu))) as tien_chiet_khau_sp_shopee,
--     ops.gia_goc,
--     ops.seller_tro_gia,
--     ops.so_tien_hoan_tra,
--     ops.tro_gia_shopee,
--     (ops.gia_goc + (ops.seller_tro_gia  + ops.so_tien_hoan_tra -ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu)) as doanh_thu_shopee,
--     (f.commission_fee *-1) + (f.service_fee *-1) + (f.seller_transaction_fee *-1) + (f.order_ams_commission_fee *-1) AS phu_phi,
--     ops.gia_von
-- FROM {{ ref('t1_shopee_shop_fee_total') }} f
-- LEFT JOIN {{ ref('t1_shopee_shop_wallet_total') }} vi 
--     ON f.order_id = vi.order_id 
--     AND f.brand = vi.brand
--     AND vi.transaction_tab_type = 'wallet_order_income'
-- LEFT JOIN {{ ref('t1_shopee_shop_order_detail_total') }} ord 
--     ON f.order_id = ord.order_id 
--     AND f.brand = ord.brand
-- LEFT JOIN order_product_summary ops 
--     ON f.order_id = ops.order_id 
--     AND f.brand = ops.brand
-- left join `google_sheet.bang_gia_von` as cost_price on ops.sku_code = cost_price.product_sku  

