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
  FROM {{ref("t1_shopee_shop_order_retrurn_total")}},
  UNNEST(item) AS i
),

total_amount AS (
  SELECT 
    a.order_id,
    b.brand,
    b.brand_lv1,
    SUM(i.discounted_price) AS total_tong_tien_san_pham
  FROM {{ref("t1_shopee_shop_fee_total")}} a,   
  UNNEST(items) AS i
  LEFT JOIN {{ref("t1_bang_gia_san_pham")}} AS b ON 
    TRIM(CASE 
      WHEN i.model_sku = ""
      THEN i.item_sku
      ELSE i.model_sku
    END) = TRIM(b.ma_sku)
  GROUP BY a.order_id, b.brand, b.brand_lv1
),

-- CTE tính tổng tiền sản phẩm KHÔNG bị trả hàng
total_amount_exclude_return AS (
  SELECT 
    a.order_id,
    a.brand,
    SUM(CASE 
      WHEN rd.return_id IS NULL OR rd.return_id = ""
      THEN i.discounted_price 
      ELSE 0 
    END) AS total_tong_tien_san_pham_excluding_return
  FROM {{ref("t1_shopee_shop_fee_total")}} a,   
  UNNEST(items) AS i
  LEFT JOIN return_detail rd ON a.order_id = rd.order_id 
    AND i.model_sku = rd.variation_sku 
    AND a.brand = rd.brand 
    AND rd.status = 'ACCEPTED'
  GROUP BY a.order_id, a.brand
),

order_detail AS (
    SELECT DISTINCT
        order_id,
        model_sku,
        FIRST_VALUE(promotion_type) OVER (
            PARTITION BY order_id, brand, model_sku 
            ORDER BY 
                CASE WHEN promotion_type IS NULL OR promotion_type = '' THEN 1 ELSE 2 END,
                promotion_type
        ) AS promotion_type,
        brand,
        shop,
        company
    FROM (
        SELECT 
            order_id,
            CASE 
                WHEN i.model_sku = "" THEN i.item_sku
                ELSE i.model_sku  
            END AS model_sku,
            i.promotion_type,
            brand,
            shop,
            company
        FROM {{ref("t1_shopee_shop_order_detail_total")}}
        CROSS JOIN UNNEST (item_list) AS i
    )
),

order_product_summary AS (
  SELECT 
    f.order_id,
    f.brand,
    mapping.brand_lv1,
    -- mapping.company_lv1,
    CASE 
      WHEN i.model_sku = "" THEN i.item_sku
      ELSE i.model_sku  
    END AS sku_code,
    i.item_id AS item_id,
    i.item_name,
    i.quantity_purchased,
    CASE
        WHEN ord.promotion_type = 'add_on_free_gift_sub' OR (rd.return_id IS NOT NULL AND rd.return_id != "")
        THEN 0
        ELSE COALESCE(mapping.gia_ban_daily, 0) * COALESCE(i.quantity_purchased, 0)
    END AS gia_ban_daily_total,
    CASE
        WHEN ord.promotion_type = 'add_on_free_gift_sub' OR (rd.return_id IS NOT NULL AND rd.return_id != "")
        THEN 0
        ELSE  
        COALESCE(i.original_price, 0) - COALESCE(i.seller_discount, 0) - COALESCE(i.discount_from_voucher_seller, 0)
    END AS doanh_thu_ke_toan,
    SAFE_DIVIDE(i.original_price,i.quantity_purchased) AS gia_san_pham_goc,
    SAFE_DIVIDE(i.original_price,i.quantity_purchased) * i.quantity_purchased AS gia_san_pham_goc_total,
     CASE
      WHEN rd.refund_amount = 0
      THEN rd.so_tien_hoan_tra * -1
      ELSE 0
    END AS so_tien_hoan_tra,
    CASE
        WHEN ord.promotion_type = 'add_on_free_gift_sub'
        THEN "Quà tặng"
        ELSE "Hàng bán"
    END AS promotion_type,
    0 AS nguoi_ban_hoan_xu,
    shopee_discount AS tro_gia_shopee,
    COALESCE(i.original_price, 0) AS gia_goc,
    COALESCE(i.seller_discount, 0)*-1 AS seller_tro_gia,
    CASE
        WHEN ord.promotion_type = 'add_on_free_gift_sub'
            THEN 0
        WHEN (rd.return_id IS NOT NULL AND rd.return_id != "")
            THEN 0
        ELSE COALESCE(cost_price.cost_price, 0)
    END AS gia_von,
    CASE
        WHEN ord.promotion_type = 'add_on_free_gift_sub'
            THEN 0
        WHEN (rd.return_id IS NOT NULL AND rd.return_id != "")
            THEN 0
        ELSE COALESCE(cost_price.cost_price, 0) * i.quantity_purchased
    END AS gia_von_total,
    COALESCE(i.discounted_price,0) AS discounted_price,
    COALESCE(rd.return_id,"") AS return_id,
    -- THÊM ROW_NUMBER ĐỂ XỬ LÝ TRONG CTE NÀY LUÔN
    ROW_NUMBER() OVER (
      PARTITION BY f.order_id, f.brand 
      ORDER BY 
        CASE WHEN i.model_sku = "" THEN i.item_sku ELSE i.model_sku END DESC
    ) AS item_rank_for_all_returned

  FROM {{ref("t1_shopee_shop_fee_total")}} f,
  UNNEST(items) AS i
  LEFT JOIN {{ref("t1_bang_gia_san_pham")}} AS mapping ON 
    CASE 
      WHEN i.model_sku = "" THEN i.item_sku
      ELSE i.model_sku  
    END = mapping.ma_sku --AND f.brand = mapping.brand
  LEFT JOIN return_detail rd ON f.order_id = rd.order_id AND i.model_sku = rd.variation_sku AND f.brand = rd.brand AND rd.status = 'ACCEPTED'
  LEFT JOIN order_detail ord ON f.order_id = ord.order_id  AND
    CASE 
        WHEN i.model_sku = "" THEN i.item_sku
        ELSE i.model_sku  
    END = ord.model_sku
    AND f.brand = ord.brand
  LEFT JOIN `google_sheet.bang_gia_von` AS cost_price ON 
    CASE 
        WHEN i.model_sku = "" THEN i.item_sku
        ELSE i.model_sku  
    END = cost_price.product_sku
), 

a AS (
SELECT 
    f.brand,
    ops.brand_lv1,
    ops.discounted_price,
    -- ops.company_lv1,
    f.company,
    f.shop,
    f.shop_id,
    DATETIME_ADD(ord.ship_by_date, INTERVAL 7 HOUR) AS ngay_ship,
   
    CASE
        WHEN LOWER(ord.order_status) IN ('cancelled', 'in_cancel') THEN 'Đã hủy'
        WHEN LOWER(ord.order_status) IN ('ready_to_ship', 'processed') THEN 'Đăng đơn'
        WHEN LOWER(ord.order_status) = 'to_confirm_receive' THEN 'Đăng đơn'
        WHEN LOWER(ord.order_status) = 'to_return' THEN 'Đã hoàn'
        WHEN LOWER(ord.order_status) = 'unpaid' THEN 'Đăng đơn'
        WHEN LOWER(ord.order_status) IN ('completed', 'shipped') THEN 'Đã giao thành công'
        ELSE ""
    END AS status,
    
    GENERATE_UUID() AS ma_giao_dich,
    'ORDER' AS don_hang_san_pham,

    f.order_id,
    f.tax_code AS ma_so_thue,
    vi.refund_sn AS ma_yeu_cau_hoan_tien,
    ops.sku_code AS ma_san_pham,
    ops.item_id AS model_id,
    ops.item_name AS ten_san_pham,
    ops.quantity_purchased AS so_luong,
    DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) AS ngay_dat_hang,
    DATETIME_ADD(vi.create_time, INTERVAL 7 HOUR) AS ngay_tien_ve_vi,
    "Ví ShopeePay" AS phuong_thuc_thanh_toan,
    'Đơn thường' AS loai_don_hang,
    
    -- LOGIC ĐƠN GIẢN HÓA - KHÔNG CẦN CTE PHỨC TẠP
    -- Mã giảm giá (voucher_from_seller)
    CASE 
        -- Trường hợp bình thường: có sản phẩm không bị return
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN 0
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * (f.voucher_from_seller * -1)
        -- Trường hợp đặc biệt: tất cả sản phẩm bị return → ghi nhận cho sản phẩm đầu tiên  
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN f.voucher_from_seller * -1
        ELSE 0
    END AS ma_giam_gia,

    
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN 0
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * (f.shopee_shipping_rebate * -1)
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN f.shopee_shipping_rebate * -1
        ELSE 0
    END AS phi_van_chuyen_tro_gia_tu_san,
    
    -- Phí cố định (commission_fee)  
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN 0
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * (f.commission_fee * -1)
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN f.commission_fee * -1
        ELSE 0
    END AS phi_co_dinh,
    
    -- Phí dịch vụ (service_fee)
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN 0
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * (f.service_fee * -1)
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN f.service_fee * -1
        ELSE 0
    END AS phi_dich_vu,
    
    -- Phí thanh toán (seller_transaction_fee)
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN 0
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * (f.seller_transaction_fee * -1)
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN f.seller_transaction_fee * -1
        ELSE 0
    END AS phi_thanh_toan,
    
    -- Phí hoa hồng tiếp thị liên kết (order_ams_commission_fee)
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN 0
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * (f.order_ams_commission_fee * -1)
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN f.order_ams_commission_fee * -1
        ELSE 0
    END AS phi_hoa_hong_tiep_thi_lien_ket,
    
    -- Ngân hàng khuyến mãi thẻ tín dụng (credit_card_promotion)
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN 0
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.credit_card_promotion
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN f.credit_card_promotion
        ELSE 0
    END AS ngan_hang_khuyen_mai_the_tin_dung,

    
    
    -- Lấy từ wallet
    vi.amount AS wallet_amount,
    
    -- Lấy từ order summary
    ops.gia_ban_daily_total,
    ops.gia_san_pham_goc,
    ops.gia_san_pham_goc_total,
    ops.doanh_thu_ke_toan,
    ops.doanh_thu_ke_toan AS doanh_thu_ke_toan_v2,
    (ops.gia_ban_daily_total - ops.doanh_thu_ke_toan) AS tien_chiet_khau_sp,
    (ops.gia_ban_daily_total -(ops.gia_goc + (ops.seller_tro_gia + ops.so_tien_hoan_tra - ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu))) AS tien_chiet_khau_sp_shopee,
    ops.gia_goc,
    ops.seller_tro_gia,
    ops.so_tien_hoan_tra,
    ops.tro_gia_shopee,
    (ops.gia_goc + (ops.seller_tro_gia + ops.so_tien_hoan_tra - ops.tro_gia_shopee) + (ops.tro_gia_shopee + ((f.voucher_from_seller)*-1) + nguoi_ban_hoan_xu)) AS doanh_thu_shopee,
    
    -- Tổng phụ phí với logic đơn giản
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return)* f.rsf_seller_protection_fee_claim_amount)
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.commission_fee * -1) +
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.service_fee * -1) +
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.seller_transaction_fee * -1) +
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.order_ams_commission_fee * -1) 
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN 
            (f.commission_fee * -1) + (f.service_fee * -1) + (f.seller_transaction_fee * -1) + (f.order_ams_commission_fee * -1) + COALESCE(f.rsf_seller_protection_fee_claim_amount)* -1
        ELSE (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return)* f.rsf_seller_protection_fee_claim_amount) * -1
    END AS phu_phi,
    
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return)* f.rsf_seller_protection_fee_claim_amount)
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.commission_fee * -1) +
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.service_fee * -1) +
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.seller_transaction_fee * -1) +
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.order_ams_commission_fee * -1) + 

            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.buyer_paid_shipping_fee) + 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.shopee_shipping_rebate) + 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.actual_shipping_fee * -1) + 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.reverse_shipping_fee * -1) + 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.final_return_to_seller_shipping_fee * -1) + 
            
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.withholding_vat_tax * -1) + 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.withholding_pit_tax * -1) 
        
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN 
            (f.commission_fee * -1) + (f.service_fee * -1) + (f.seller_transaction_fee * -1) + (f.order_ams_commission_fee * -1) + COALESCE(f.rsf_seller_protection_fee_claim_amount)* -1 + 
            (f.buyer_paid_shipping_fee)  + (f.shopee_shipping_rebate)  + (f.actual_shipping_fee * -1)  + (f.reverse_shipping_fee * -1) + (f.final_return_to_seller_shipping_fee * -1) + (f.withholding_vat_tax * -1) + (f.withholding_pit_tax * -1)
        ELSE (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return)* f.rsf_seller_protection_fee_claim_amount) * -1
    END AS tong_chi_phi,

    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.withholding_vat_tax * -1) + 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * f.withholding_pit_tax * -1) 
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN 
            (f.withholding_vat_tax * -1) + (f.withholding_pit_tax * -1)
    END AS tax,
    f.withholding_vat_tax * -1  as  thue_gtgt,
    f.withholding_pit_tax * -1 as thue_tncn,
    
    -- CASE 
    --     WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
    --     THEN  ops.gia_von 
    --     WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
    --     THEN ops.gia_von *-1
    --     WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
    --     THEN 
    --        ops.gia_von *-1
    --     ELSE ops.gia_von *-1
    -- END AS gia_von,

    -- CASE 
    --     WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
    --     THEN  ops.gia_von_total 
    --     WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
    --     THEN ops.gia_von_total *-1
    --     WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
    --     THEN 
    --        ops.gia_von_total *-1
    --     ELSE ops.gia_von_total *-1
    -- END AS gia_von_total,

    ops.gia_von,
    ops.gia_von_total,
    ops.promotion_type,
    
    -- Phí vận chuyển thực tế
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return)* f.rsf_seller_protection_fee_claim_amount)
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN 
            (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * actual_shipping_fee)
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN actual_shipping_fee - buyer_paid_shipping_fee
        
        ELSE 0
    END AS phi_van_chuyen_thuc_te,
    
    -- Tổng tiền đã thanh toán
    CASE 
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') != ''
        THEN COALESCE(f.rsf_seller_protection_fee_claim_amount)
        WHEN tae.total_tong_tien_san_pham_excluding_return > 0 AND COALESCE(ops.return_id, '') = ''
        THEN (SAFE_DIVIDE(ops.discounted_price, tae.total_tong_tien_san_pham_excluding_return) * vi.amount) + COALESCE(f.rsf_seller_protection_fee_claim_amount)* -1
        WHEN tae.total_tong_tien_san_pham_excluding_return = 0 AND ops.item_rank_for_all_returned = 1
        THEN vi.amount + COALESCE(f.rsf_seller_protection_fee_claim_amount)* -1
        ELSE COALESCE(f.rsf_seller_protection_fee_claim_amount,0) * -1
    END AS tong_tien_da_thanh_toan

FROM {{ref("t1_shopee_shop_fee_total")}} f
LEFT JOIN {{ref("t1_shopee_shop_wallet_total")}} vi 
    ON f.order_id = vi.order_id 
    AND f.brand = vi.brand
    AND vi.transaction_tab_type = 'wallet_order_income'
LEFT JOIN {{ref("t1_shopee_shop_order_detail_total")}} ord 
    ON f.order_id = ord.order_id 
    AND f.brand = ord.brand
LEFT JOIN order_product_summary ops 
    ON f.order_id = ops.order_id 
    AND f.brand = ops.brand
LEFT JOIN total_amount ta ON ta.order_id = f.order_id AND ta.brand = f.brand
LEFT JOIN total_amount_exclude_return tae ON tae.order_id = f.order_id AND tae.brand = f.brand
-- BỎ JOIN với order_with_all_returned CTE để tránh duplicate
)

select * from a -- where order_id ="250803T3CP2AY2"