WITH LineItems AS (
  SELECT
    o.brand,
    mapping.brand_lv1,
    -- mapping.company_lv1,
    o.order_id,
    o.company,
    o.shop,
    JSON_VALUE(li, '$.sku_id') AS SKU_ID,
    JSON_VALUE(li, '$.seller_sku') AS Seller_SKU,
    JSON_VALUE(li, '$.product_name') AS Product_Name,
    JSON_VALUE(li, '$.sku_name') AS Variation,
    JSON_VALUE(li, '$.sku_type') AS Normal_or_Preorder,
    CAST(JSON_VALUE(li, '$.is_gift') AS BOOL) AS is_gift,
    JSON_VALUE(li, '$.cancel_reason') AS SKU_Cancel_Reason,
    JSON_VALUE(li, '$.display_status') AS SKU_Display_Status,
    COUNT(*) AS Quantity,
    CAST(JSON_VALUE(li, '$.original_price') AS FLOAT64) AS SKU_Unit_Original_Price,
    SUM(CAST(JSON_VALUE(li, '$.original_price') AS FLOAT64)) AS SKU_Subtotal_Before_Discount,
    SUM(CAST(JSON_VALUE(li, '$.platform_discount') AS FLOAT64)) AS SKU_Platform_Discount,
    SUM(CAST(JSON_VALUE(li, '$.seller_discount') AS FLOAT64)) AS SKU_Seller_Discount,
    SUM(CAST(JSON_VALUE(li, '$.sale_price') AS FLOAT64)) AS SKU_Subtotal_After_Discount,
    CAST(JSON_VALUE(li, '$.sale_price') AS FLOAT64) AS SKU_Refund_Amount,
    JSON_VALUE(li, '$.package_id') AS Package_ID,
    mapping.gia_ban_daily AS Gia_Ban_Daily,
    cost_price.cost_price as gia_von,
    "order_line" as line_type 
  FROM {{ref("t1_tiktok_order_tot")}} o
  CROSS JOIN UNNEST(o.line_items) AS li
  LEFT JOIN {{ref("t1_bang_gia_san_pham")}} AS mapping
    ON JSON_VALUE(li, '$.seller_sku') = mapping.ma_sku --and o.brand = mapping.brand
  left join `google_sheet.bang_gia_von` as cost_price on JSON_VALUE(li, '$.seller_sku') = cost_price.product_sku
  GROUP BY
    o.brand,
    o.shop,
    o.order_id,
    o.company,
    JSON_VALUE(li, '$.sku_id'),
    JSON_VALUE(li, '$.seller_sku'),
    JSON_VALUE(li, '$.product_name'),
    JSON_VALUE(li, '$.sku_name'),
    JSON_VALUE(li, '$.sku_type'),
    CAST(JSON_VALUE(li, '$.is_gift') AS BOOL),
    JSON_VALUE(li, '$.cancel_reason'),
    JSON_VALUE(li, '$.display_status'),
    CAST(JSON_VALUE(li, '$.original_price') AS FLOAT64),
    CAST(JSON_VALUE(li, '$.sale_price') AS FLOAT64),
    JSON_VALUE(li, '$.package_id'),
    mapping.gia_ban_daily,
    mapping.brand_lv1,
    -- mapping.company_lv1,
    cost_price.cost_price
),


ReturnLineItems AS (
  SELECT
    r.order_id,
    r.brand,
    r.shop,
    JSON_VALUE(li, '$.sku_id') AS SKU_ID,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) AS Sku_Quantity_of_Return,
    CAST(JSON_VALUE(r.refund_amount, '$.refund_total') AS FLOAT64) AS Order_Refund_Amount,
    CASE r.return_status 
        WHEN 'RETURN_OR_REFUND_REQUEST_COMPLETE' THEN 'return_refund'
        ELSE null
    END AS Cancelation_Return_Type,
    "return_line" as line_type 
  FROM {{ref("t1_tiktok_order_return")}} r
  CROSS JOIN UNNEST(r.return_line_items) AS li
  where r.return_status = 'RETURN_OR_REFUND_REQUEST_COMPLETE'
),



-- Tạo OrderData cho các dòng order thông thường
OrderData AS (
  SELECT
    li.brand,
    li.brand_lv1,
    li.shop,
    li.company,
    li.order_id AS Order_ID,
    CASE o.order_status
      WHEN 'CANCELLED' THEN 'Canceled'
      WHEN 'DELIVERED' THEN 'Shipped'
      ELSE o.order_status
    END AS Order_Status,
    CASE o.order_status
      WHEN 'CANCELLED' THEN 'Canceled'
      WHEN 'DELIVERED' THEN 'Delivered'
      ELSE o.order_status
    END AS Order_Substatus,
    CASE 
      WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.SKU_Display_Status = 'CANCELLED' THEN 'Cancel'
      ELSE NULL
    END AS Cancelation_Return_Type,
    li.Normal_or_Preorder,
    li.SKU_ID,
    li.Seller_SKU,
    li.Product_Name,
    li.Variation,
    li.Quantity,
    CASE 
      WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.is_gift = FALSE AND li.SKU_Display_Status = 'CANCELLED' THEN li.Quantity
      ELSE 0
    END AS Sku_Quantity_of_Return,
    li.SKU_Unit_Original_Price,
    li.SKU_Subtotal_Before_Discount,
    li.SKU_Platform_Discount,
    li.SKU_Seller_Discount,
    li.SKU_Subtotal_After_Discount,
    li.Gia_Ban_Daily,
    li.gia_von,
    li.is_gift,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee') AS FLOAT64) AS Shipping_Fee_After_Discount,
    CAST(JSON_VALUE(o.payment, '$.original_shipping_fee') AS FLOAT64) AS Original_Shipping_Fee,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee_seller_discount') AS FLOAT64) AS Shipping_Fee_Seller_Discount,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee_platform_discount') AS FLOAT64) AS Shipping_Fee_Platform_Discount,
    0 AS Payment_Platform_Discount,
    CAST(JSON_VALUE(o.payment, '$.tax') AS FLOAT64) AS Taxes,
    CAST(JSON_VALUE(o.payment, '$.total_amount') AS FLOAT64) AS Order_Amount,
    CASE 
      WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.SKU_Display_Status = 'CANCELLED' THEN li.SKU_Refund_Amount
      ELSE NULL
    END AS Order_Refund_Amount,
    DATETIME_ADD(o.create_time, INTERVAL 7 HOUR) AS Created_Time,
    DATETIME_ADD(o.paid_time, INTERVAL 7 HOUR) AS Paid_Time,
    DATETIME_ADD(o.rts_time, INTERVAL 7 HOUR) AS RTS_Time,
    DATETIME_ADD(o.collection_time, INTERVAL 7 HOUR) AS Shipped_Time,
    DATETIME_ADD(o.delivery_time, INTERVAL 7 HOUR) AS Delivered_Time,
    DATETIME_ADD(o.cancel_time, INTERVAL 7 HOUR) AS Cancelled_Time,
    CASE o.cancellation_initiator
      WHEN 'BUYER' THEN 'User'
      ELSE o.cancellation_initiator
    END AS Cancel_By,
    CASE li.SKU_Cancel_Reason
      WHEN 'Không còn nhu cầu' THEN 'No longer needed'
      ELSE li.SKU_Cancel_Reason
    END AS Cancel_Reason,
    CASE o.fulfillment_type
      WHEN 'FULFILLMENT_BY_SELLER' THEN 'Fulfillment by seller'
      ELSE o.fulfillment_type
    END AS Fulfillment_Type,
    CASE o.warehouse_id
      WHEN '7414347696732063494' THEN 'BH'
      ELSE NULL
    END AS Warehouse_Name,
    o.tracking_number AS Tracking_ID,
    CASE o.delivery_option_name
      WHEN 'Standard shipping' THEN 'Vận chuyển tiêu chuẩn'
      ELSE o.delivery_option_name
    END AS Delivery_Option,
    o.shipping_provider AS Shipping_Provider_Name,
    o.buyer_message AS Buyer_Message,
    o.buyer_email AS Buyer_Username,
    o.order_type,
    JSON_VALUE(o.recipient_address, '$.name') AS Recipient,
    JSON_VALUE(o.recipient_address, '$.phone_number') AS Phone_Number,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L0') AS Country,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L1') AS Province,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L2') AS District,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L3') AS Commune,
    JSON_VALUE(o.recipient_address, '$.address_detail') AS Detail_Address,
    JSON_VALUE(o.recipient_address, '$.address_line2') AS Additional_Address_Information,
    CASE o.payment_method_name
      WHEN 'Cash on delivery' THEN 'Thanh toán khi giao hàng'
      ELSE o.payment_method_name
    END AS Payment_Method,
    NULL AS Weight_kg,
     CAST(NULL AS STRING) AS Product_Category,
    li.Package_ID,
    o.seller_note AS Seller_Note,
    'Unchecked' AS Checked_Status,
    CAST(NULL AS STRING) AS Checked_Marked_by,
    'order_line' AS line_type
  FROM LineItems li
  JOIN {{ref("t1_tiktok_order_tot")}} o
    ON li.order_id = o.order_id
    AND li.brand = o.brand
),


-- Tạo ReturnData cho các dòng return
ReturnData AS (
  SELECT
    o.brand,
    mapping.brand_lv1,
    o.shop,
    o.company,
    r.order_id AS Order_ID,
    'RETURNED' AS Order_Status,
    'Return Completed' AS Order_Substatus,
    'return_refund' AS Cancelation_Return_Type,
    JSON_VALUE(o_li, '$.sku_type') AS Normal_or_Preorder,
    JSON_VALUE(li, '$.sku_id') AS SKU_ID,
    JSON_VALUE(o_li, '$.seller_sku') AS Seller_SKU,
    JSON_VALUE(o_li, '$.product_name') AS Product_Name,
    JSON_VALUE(o_li, '$.sku_name') AS Variation,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) AS Quantity,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) AS Sku_Quantity_of_Return,
    CAST(JSON_VALUE(o_li, '$.original_price') AS FLOAT64) AS SKU_Unit_Original_Price,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) * CAST(JSON_VALUE(o_li, '$.original_price') AS FLOAT64) AS SKU_Subtotal_Before_Discount,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) * CAST(JSON_VALUE(o_li, '$.platform_discount') AS FLOAT64) AS SKU_Platform_Discount,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) * CAST(JSON_VALUE(o_li, '$.seller_discount') AS FLOAT64) AS SKU_Seller_Discount,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) * CAST(JSON_VALUE(o_li, '$.sale_price') AS FLOAT64) AS SKU_Subtotal_After_Discount,
    mapping.gia_ban_daily AS Gia_Ban_Daily,
    cost_price.cost_price as gia_von,
    FALSE AS is_gift,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee') AS FLOAT64) AS Shipping_Fee_After_Discount,
    CAST(JSON_VALUE(o.payment, '$.original_shipping_fee') AS FLOAT64) AS Original_Shipping_Fee,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee_seller_discount') AS FLOAT64) AS Shipping_Fee_Seller_Discount,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee_platform_discount') AS FLOAT64) AS Shipping_Fee_Platform_Discount,
    0 AS Payment_Platform_Discount,
    CAST(JSON_VALUE(o.payment, '$.tax') AS FLOAT64) AS Taxes,
    CAST(JSON_VALUE(o.payment, '$.total_amount') AS FLOAT64) AS Order_Amount,
    CAST(JSON_VALUE(r.refund_amount, '$.refund_total') AS FLOAT64) AS Order_Refund_Amount,
    
    DATETIME_ADD(o.create_time, INTERVAL 7 HOUR) AS Created_Time,
    DATETIME_ADD(o.paid_time, INTERVAL 7 HOUR) AS Paid_Time,
    DATETIME_ADD(o.rts_time, INTERVAL 7 HOUR) AS RTS_Time,
    DATETIME_ADD(o.collection_time, INTERVAL 7 HOUR) AS Shipped_Time,
    DATETIME_ADD(o.delivery_time, INTERVAL 7 HOUR) AS Delivered_Time,
    DATETIME_ADD(o.cancel_time, INTERVAL 7 HOUR) AS Cancelled_Time,
    
    'BUYER' AS Cancel_By,
    r.return_reason AS Cancel_Reason,
    CASE o.fulfillment_type
      WHEN 'FULFILLMENT_BY_SELLER' THEN 'Fulfillment by seller'
      ELSE o.fulfillment_type
    END AS Fulfillment_Type,
    CASE o.warehouse_id
      WHEN '7414347696732063494' THEN 'BH'
      ELSE NULL
    END AS Warehouse_Name,
    o.tracking_number AS Tracking_ID,
    CASE o.delivery_option_name
      WHEN 'Standard shipping' THEN 'Vận chuyển tiêu chuẩn'
      ELSE o.delivery_option_name
    END AS Delivery_Option,
    o.shipping_provider AS Shipping_Provider_Name,
    o.buyer_message AS Buyer_Message,
    o.buyer_email AS Buyer_Username,
    o.order_type,
    JSON_VALUE(o.recipient_address, '$.name') AS Recipient,
    JSON_VALUE(o.recipient_address, '$.phone_number') AS Phone_Number,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L0') AS Country,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L1') AS Province,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L2') AS District,
    (SELECT JSON_VALUE(d, '$.address_name')
     FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
     WHERE JSON_VALUE(d, '$.address_level') = 'L3') AS Commune,
    JSON_VALUE(o.recipient_address, '$.address_detail') AS Detail_Address,
    JSON_VALUE(o.recipient_address, '$.address_line2') AS Additional_Address_Information,
    CASE o.payment_method_name
      WHEN 'Cash on delivery' THEN 'Thanh toán khi giao hàng'
      ELSE o.payment_method_name
    END AS Payment_Method,
    
    CAST(NULL AS FLOAT64) AS Weight_kg,
    CAST(NULL AS STRING) AS Product_Category,
    
    JSON_VALUE(o_li, '$.package_id') AS Package_ID,
    o.seller_note AS Seller_Note,
    'Unchecked' AS Checked_Status,
    CAST(NULL AS STRING) AS Checked_Marked_by,
    'return_line' AS line_type
    
  -- *** QUAN TRỌNG: Thứ tự đúng là FROM -> JOIN -> WHERE ***
  FROM {{ref("t1_tiktok_order_return")}} r
  CROSS JOIN UNNEST(r.return_line_items) AS li
  JOIN {{ref("t1_tiktok_order_tot")}} o
    ON r.order_id = o.order_id
    AND r.brand = o.brand
  CROSS JOIN UNNEST(o.line_items) AS o_li
  -- Di chuyển LEFT JOINs lên đây, TRƯỚC WHERE
  LEFT JOIN {{ref("t1_bang_gia_san_pham")}} AS mapping
    ON JSON_VALUE(o_li, '$.seller_sku') = mapping.ma_sku
  LEFT JOIN `google_sheet.bang_gia_von` as cost_price 
    ON JSON_VALUE(o_li, '$.seller_sku') = cost_price.product_sku
  -- WHERE phải đặt SAU tất cả các JOINs
  WHERE JSON_VALUE(li, '$.sku_id') = JSON_VALUE(o_li, '$.sku_id')
    AND r.return_status = 'RETURN_OR_REFUND_REQUEST_COMPLETE'
),


-- UNION ALL để kết hợp order lines và return lines
CombinedData AS (
  SELECT * FROM OrderData
  UNION ALL
  SELECT * FROM ReturnData
),


OrderTotal as (
SELECT
    brand,
    shop,
    Order_ID,
    line_type,
    sum(SKU_Subtotal_After_Discount) as tong_tien_sau_giam_gia
FROM CombinedData
GROUP BY
    brand,
    shop,
    Order_ID,
    line_type
),


orderLine as(
  SELECT
    brand,
    brand_lv1,
    shop,
    company,
    Order_ID as ma_don_hang,
    Order_Status,
    Order_Substatus,
    Cancelation_Return_Type,
    Normal_or_Preorder,
    SKU_ID,
    Seller_SKU as sku_code,
    Product_Name as ten_san_pham,
    Variation,
    Quantity as so_luong,
    Sku_Quantity_of_Return,
    SKU_Unit_Original_Price,
    SKU_Subtotal_Before_Discount,
    SKU_Platform_Discount as san_tro_gia,
    SKU_Seller_Discount as seller_tro_gia,
    0 as giam_gia_seller,
    0 as giam_gia_san,
    SKU_Subtotal_After_Discount,
    Shipping_Fee_After_Discount,
    Original_Shipping_Fee,
    Shipping_Fee_Seller_Discount,
    Shipping_Fee_Platform_Discount,
    Payment_Platform_Discount,
    Taxes,
    Order_Amount,
    Order_Refund_Amount,
    Created_Time as ngay_tao_don,
    Paid_Time,
    RTS_Time,
    Shipped_Time,
    Delivered_Time,
    Cancelled_Time,
    Cancel_By,
    Cancel_Reason,
    Fulfillment_Type,
    Warehouse_Name,
    Tracking_ID,
    Delivery_Option,
    Shipping_Provider_Name,
    Buyer_Message,
    Buyer_Username,
    Recipient,
    Phone_Number,
    Country,
    Province,
    District,
    Commune,
    Detail_Address,
    Additional_Address_Information,
    Payment_Method,
    Weight_kg,
    Product_Category,
    Package_ID,
    Seller_Note,
    Checked_Status,
    Checked_Marked_by,
    Gia_Ban_Daily AS gia_ban_daily,
    COALESCE(SKU_Unit_Original_Price, 0) AS gia_san_pham_goc,
    COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0) AS gia_san_pham_goc_total,
    (COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0) AS tien_sp_sau_tro_gia,
    
    CASE
        WHEN line_type = 'return_line' THEN Gia_Ban_Daily * Quantity * -1
        WHEN Order_Status = 'Canceled' THEN 0
        ELSE Gia_Ban_Daily * Quantity
    END AS gia_ban_daily_total,


    CASE
        WHEN line_type = 'return_line' THEN (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) * -1
        WHEN Order_Status = 'Canceled' THEN 0
        ELSE (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))
    END AS tien_chiet_khau_sp,


    CASE
        WHEN line_type = 'return_line' THEN ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) * -1
        WHEN Order_Status = 'Canceled' THEN 0
        ELSE ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))
    END AS doanh_thu_ke_toan,


    (COALESCE(gia_von, 0) * COALESCE(Quantity, 0)) as gia_von,


    CASE
      WHEN is_gift = TRUE THEN "Quà Tặng"
      ELSE NULL
    END as promotion_type,
    
    order_type,
    line_type,


    CASE
      WHEN line_type = 'return_line' THEN 'Đã hoàn'
      WHEN Order_Status = 'Shipped' THEN 'Đang giao'
      WHEN Order_Status = 'AWAITING_COLLECTION' THEN 'Đang giao'
      WHEN Order_Status = 'AWAITING_SHIPMENT' THEN 'Đang giao'
      WHEN Order_Status = 'Canceled' THEN 'Đã hủy'
      WHEN Order_Status = 'COMPLETED' THEN 'Đã giao thành công'
      WHEN Order_Status = 'UNPAID' THEN 'Đăng đơn'
      WHEN Order_Status = 'IN_TRANSIT' THEN 'Đang giao'
      ELSE 'Khác'
    END AS status


  FROM CombinedData
  ORDER BY Order_ID, line_type, SKU_ID
),


-- *** PHẦN QUAN TRỌNG - JOIN VỚI TRANSACTION TABLE DỰA TRÊN total_revenue ***
final_result as (
  SELECT
    ord.*,
    trans.order_statement_time,
    trans.order_adjustment_id,
    trans.adjustment_id,
    
    -- Tính total_settlement_amount theo tỷ lệ
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.total_settlement_amount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.total_settlement_amount, 0)
    END as total_settlement_amount,
    
    -- Phí vận chuyển trợ giá từ sàn (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.platform_shipping_fee_discount, 0) AS phi_van_chuyen_tro_gia_tu_san,
    
    -- Phí thanh toán (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.transaction_fee, 0) AS phi_thanh_toan,
    
    -- Phí hoa hồng shop (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.tiktok_shop_commission_fee, 0) AS phi_hoa_hong_shop,
    
    -- Phí hoa hồng tiếp thị liên kết (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_commission, 0) AS phi_hoa_hong_tiep_thi_lien_ket,
    
    -- Phí hoa hồng quảng cáo của hàng (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_shop_ads_commission, 0) AS phi_hoa_hong_quang_cao_cua_hang,
    
    -- Phí dịch vụ (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.sfp_service_fee, 0) AS phi_dich_vu,
    
    -- Phí ship (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) AS phi_ship,
    
    -- Phí xtra (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.voucher_xtra_service_fee, 0) as phi_xtra,
    
    -- Thuế GTGT (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vat_amount, 0) as thue_gtgt,
    
    -- Thuế TNCN (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.pit_amount, 0) as thue_tncn,
    
    -- Affiliate partner commission (theo tỷ lệ)
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_partner_commission, 0) as affiliate_partner_commission,
    
    0 as voucher_from_seller,
    0 as phi_co_dinh,
    
    -- Tiền khách hàng thanh toán
    ord.gia_san_pham_goc_total - ord.seller_tro_gia - ord.san_tro_gia - COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) AS tien_khach_hang_thanh_toan,


    -- Tổng phí sàn
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.tiktok_shop_commission_fee + trans.shipping_cost_amount + trans.affiliate_commission + trans.affiliate_shop_ads_commission + trans.sfp_service_fee + trans.voucher_xtra_service_fee + trans.vat_amount + trans.pit_amount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.tiktok_shop_commission_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_cost_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_commission, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_shop_ads_commission, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_partner_commission, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.sfp_service_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.voucher_xtra_service_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vat_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.pit_amount, 0) 
    END as tong_phi_san,


    -- Tax
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.vat_amount + trans.pit_amount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vat_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.pit_amount, 0) 
    END as tax,


    -- Phí vận chuyển thực tế
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.shipping_cost_amount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_cost_amount, 0)
    END as phi_van_chuyen_thuc_te,


    -- Seller shipping fee
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.actual_shipping_fee + trans.platform_shipping_fee_discount + trans.customer_shipping_fee + trans.actual_return_shipping_fee + trans.refunded_customer_shipping_fee_amount + trans.failed_delivery_subsidy_amount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_shipping_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.platform_shipping_fee_discount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_return_shipping_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.refunded_customer_shipping_fee_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.failed_delivery_subsidy_amount, 0)
    END as seller_shipping_fee,


    -- Actual shipping fee
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.actual_shipping_fee
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_shipping_fee, 0)
    END as actual_shipping_fee,


    -- Platform shipping fee discount
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.platform_shipping_fee_discount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.platform_shipping_fee_discount, 0)
    END as platform_shipping_fee_discount,


    -- Customer shipping fee
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.customer_shipping_fee
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0)
    END as customer_shipping_fee,


    -- Actual return shipping fee
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.actual_return_shipping_fee
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_return_shipping_fee, 0)
    END as actual_return_shipping_fee,


    -- Refunded customer shipping fee amount
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.refunded_customer_shipping_fee_amount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.refunded_customer_shipping_fee_amount, 0)
    END as refunded_customer_shipping_fee_amount,


    -- Failed delivery subsidy amount
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN trans.failed_delivery_subsidy_amount
        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.failed_delivery_subsidy_amount, 0)
    END as failed_delivery_subsidy_amount


  FROM orderLine ord
  LEFT JOIN OrderTotal total 
    ON ord.brand = total.brand 
    AND ord.ma_don_hang = total.Order_ID
    AND ord.line_type = total.line_type
  
  -- *** THAY ĐỔI QUAN TRỌNG: JOIN DỰA TRÊN total_revenue ***
  LEFT JOIN {{ref("t2_tiktok_brand_statement_transaction_order_tot")}} trans 
    ON ord.brand = trans.brand 
    AND ord.ma_don_hang = trans.order_adjustment_id
    AND (
      -- Mapping cho order lines: total_revenue >= 0 (transaction dương)
      (ord.line_type = 'order_line' AND COALESCE(trans.total_revenue, 0) >= 0)
      OR
      -- Mapping cho return lines: total_revenue < 0 (transaction âm - refund)
      (ord.line_type = 'return_line' AND trans.total_revenue < 0)
    )
)


SELECT * FROM final_result
















-- OrderData AS (
--   SELECT
--     li.brand,
--     li.brand_lv1,
--     -- li.company_lv1,
--     li.shop,
--     li.company,
--     li.order_id AS Order_ID,
--     CASE o.order_status
--       WHEN 'CANCELLED' THEN 'Canceled'
--       WHEN 'DELIVERED' THEN 'Shipped'
--       ELSE o.order_status
--     END AS Order_Status,
--     CASE o.order_status
--       WHEN 'CANCELLED' THEN 'Canceled'
--       WHEN 'DELIVERED' THEN 'Delivered'
--       ELSE o.order_status
--     END AS Order_Substatus,
--     -- Map return info nếu có, ưu tiên Cancelation_Return_Type từ return
--     COALESCE(r.Cancelation_Return_Type,
--       CASE 
--         WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.SKU_Display_Status = 'CANCELLED' THEN 'Cancel'
--         ELSE NULL
--       END) AS Cancelation_Return_Type,
--     li.Normal_or_Preorder,
--     li.SKU_ID,
--     li.Seller_SKU,
--     li.Product_Name,
--     li.Variation,
--     li.Quantity,
--     -- Nếu có return thì lấy số lượng return, không thì lấy theo cancel logic cũ
--     COALESCE(r.Sku_Quantity_of_Return,
--       CASE 
--         WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.is_gift = FALSE AND li.SKU_Display_Status = 'CANCELLED' THEN li.Quantity
--         ELSE 0
--       END) AS Sku_Quantity_of_Return,
--     li.SKU_Unit_Original_Price,
--     li.SKU_Subtotal_Before_Discount,
--     li.SKU_Platform_Discount,
--     li.SKU_Seller_Discount,
--     li.SKU_Subtotal_After_Discount,
--     li.Gia_Ban_Daily,
--     li.gia_von,
--     li.is_gift,
--     CAST(JSON_VALUE(o.payment, '$.shipping_fee') AS FLOAT64) AS Shipping_Fee_After_Discount,
--     CAST(JSON_VALUE(o.payment, '$.original_shipping_fee') AS FLOAT64) AS Original_Shipping_Fee,
--     CAST(JSON_VALUE(o.payment, '$.shipping_fee_seller_discount') AS FLOAT64) AS Shipping_Fee_Seller_Discount,
--     CAST(JSON_VALUE(o.payment, '$.shipping_fee_platform_discount') AS FLOAT64) AS Shipping_Fee_Platform_Discount,
--     0 AS Payment_Platform_Discount,
--     CAST(JSON_VALUE(o.payment, '$.tax') AS FLOAT64) AS Taxes,
--     CAST(JSON_VALUE(o.payment, '$.total_amount') AS FLOAT64) AS Order_Amount,
--     -- Lấy tiền hoàn trả từ return nếu có, nếu không thì theo logic cũ
--     COALESCE(r.Order_Refund_Amount,
--       CASE 
--         WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.SKU_Display_Status = 'CANCELLED' THEN li.SKU_Refund_Amount
--         ELSE NULL
--       END) AS Order_Refund_Amount,
--     DATETIME_ADD(o.create_time, INTERVAL 7 HOUR) AS Created_Time,
--     DATETIME_ADD(o.paid_time, INTERVAL 7 HOUR) AS Paid_Time,
--     DATETIME_ADD(o.rts_time, INTERVAL 7 HOUR) AS RTS_Time,
--     DATETIME_ADD(o.collection_time, INTERVAL 7 HOUR) AS Shipped_Time,
--     DATETIME_ADD(o.delivery_time, INTERVAL 7 HOUR) AS Delivered_Time,
--     DATETIME_ADD(o.cancel_time, INTERVAL 7 HOUR) AS Cancelled_Time,
--     CASE o.cancellation_initiator
--       WHEN 'BUYER' THEN 'User'
--       ELSE o.cancellation_initiator
--     END AS Cancel_By,
--     CASE li.SKU_Cancel_Reason
--       WHEN 'Không còn nhu cầu' THEN 'No longer needed'
--       ELSE li.SKU_Cancel_Reason
--     END AS Cancel_Reason,
--     CASE o.fulfillment_type
--       WHEN 'FULFILLMENT_BY_SELLER' THEN 'Fulfillment by seller'
--       ELSE o.fulfillment_type
--     END AS Fulfillment_Type,
--     CASE o.warehouse_id
--       WHEN '7414347696732063494' THEN 'BH'
--       ELSE NULL
--     END AS Warehouse_Name,
--     o.tracking_number AS Tracking_ID,
--     CASE o.delivery_option_name
--       WHEN 'Standard shipping' THEN 'Vận chuyển tiêu chuẩn'
--       ELSE o.delivery_option_name
--     END AS Delivery_Option,
--     o.shipping_provider AS Shipping_Provider_Name,
--     o.buyer_message AS Buyer_Message,
--     o.buyer_email AS Buyer_Username,
--     o.order_type,
--     JSON_VALUE(o.recipient_address, '$.name') AS Recipient,
--     JSON_VALUE(o.recipient_address, '$.phone_number') AS Phone_Number,
--     (SELECT JSON_VALUE(d, '$.address_name')
--      FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
--      WHERE JSON_VALUE(d, '$.address_level') = 'L0') AS Country,
--     (SELECT JSON_VALUE(d, '$.address_name')
--      FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
--      WHERE JSON_VALUE(d, '$.address_level') = 'L1') AS Province,
--     (SELECT JSON_VALUE(d, '$.address_name')
--      FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
--      WHERE JSON_VALUE(d, '$.address_level') = 'L2') AS District,
--     (SELECT JSON_VALUE(d, '$.address_name')
--      FROM UNNEST(JSON_QUERY_ARRAY(o.recipient_address, '$.district_info')) d
--      WHERE JSON_VALUE(d, '$.address_level') = 'L3') AS Commune,
--     JSON_VALUE(o.recipient_address, '$.address_detail') AS Detail_Address,
--     JSON_VALUE(o.recipient_address, '$.address_line2') AS Additional_Address_Information,
--     CASE o.payment_method_name
--       WHEN 'Cash on delivery' THEN 'Thanh toán khi giao hàng'
--       ELSE o.payment_method_name
--     END AS Payment_Method,
--     NULL AS Weight_kg,
--     NULL AS Product_Category,
--     li.Package_ID,
--     o.seller_note AS Seller_Note,
--     'Unchecked' AS Checked_Status,
--     NULL AS Checked_Marked_by
--   FROM LineItems li
--   JOIN {{ref("t1_tiktok_order_tot")}} o
--     ON li.order_id = o.order_id
--     AND li.brand = o.brand
--   LEFT JOIN ReturnLineItems r
--     ON li.order_id = r.order_id
--     AND li.SKU_ID = r.SKU_ID
--     and li.brand = r.brand
-- ),

-- OrderTotal as (
-- SELECT
--     brand,
--     shop,
--     Order_ID,
--     sum(SKU_Subtotal_After_Discount) as tong_tien_sau_giam_gia
-- FROM  OrderData
-- GROUP BY
--     brand,
--     shop,
--     Order_ID
-- ),

-- orderLine as(
-- SELECT
--   brand,
--   brand_lv1,
-- --   company_lv1,
--   shop,
--   company,
--   Order_ID as ma_don_hang,
--   Order_Status,
--   Order_Substatus,
--   Cancelation_Return_Type,
--   Normal_or_Preorder,
--   SKU_ID ,
--   Seller_SKU as sku_code,
--   Product_Name as ten_san_pham,
--   Variation,
--   Quantity as so_luong,
--   Sku_Quantity_of_Return,
--   SKU_Unit_Original_Price,
--   SKU_Subtotal_Before_Discount,
--   SKU_Platform_Discount as san_tro_gia,
--   SKU_Seller_Discount as seller_tro_gia,
--   0 as giam_gia_seller,
--   0 as giam_gia_san,
--   SKU_Subtotal_After_Discount,
--   Shipping_Fee_After_Discount,
--   Original_Shipping_Fee,
--   Shipping_Fee_Seller_Discount,
--   Shipping_Fee_Platform_Discount,
--   Payment_Platform_Discount,
--   Taxes,
--   Order_Amount,
--   Order_Refund_Amount,
--   Created_Time as ngay_tao_don,
--   Paid_Time,
--   RTS_Time,
--   Shipped_Time,
--   Delivered_Time,
--   Cancelled_Time,
--   Cancel_By,
--   Cancel_Reason,
--   Fulfillment_Type,
--   Warehouse_Name,
--   Tracking_ID,
--   Delivery_Option,
--   Shipping_Provider_Name,
--   Buyer_Message,
--   Buyer_Username,
--   Recipient,
--   Phone_Number,
--   Country,
--   Province,
--   District,
--   Commune,
--   Detail_Address,
--   Additional_Address_Information,
--   Payment_Method,
--   Weight_kg,
--   Product_Category,
--   Package_ID,
--   Seller_Note,
--   Checked_Status,
--   Checked_Marked_by,
--   Gia_Ban_Daily AS gia_ban_daily,
-- --   Gia_Ban_Daily * Quantity AS gia_ban_daily_total,
--   COALESCE(SKU_Unit_Original_Price, 0) AS gia_san_pham_goc,
--   COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0) AS gia_san_pham_goc_total,
--   (COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0) AS tien_sp_sau_tro_gia,
  
-- --   (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) AS tien_chiet_khau_sp,
  
--   (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))) AS doanh_thu_ke_toan_v2,
--   (COALESCE(gia_von, 0) * COALESCE(Quantity, 0)) as gia_von,
-- --   (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))) AS doanh_thu_ke_toan,
   
--   CASE
--       WHEN Cancelation_Return_Type = 'return_refund' THEN Gia_Ban_Daily * Quantity * -1
--       WHEN Order_Status = 'Canceled' THEN 0
--       ELSE Gia_Ban_Daily * Quantity
--   END AS gia_ban_daily_total,

--   CASE
--       WHEN Cancelation_Return_Type = 'return_refund' THEN (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) * -1
--       WHEN Order_Status = 'Canceled' THEN 0
--       ELSE (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))
--   END AS tien_chiet_khau_sp,
--   CASE
--       WHEN Cancelation_Return_Type = 'return_refund' THEN ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) * -1
--       WHEN Order_Status = 'Canceled' THEN 0
--       ELSE ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))
--   END AS doanh_thu_ke_toan,


--   case
--   when is_gift = TRUE
--   then "Quà Tặng"
--   end as promotion_type,
--   order_type,

--   CASE
--     WHEN Cancelation_Return_Type = 'return_refund' THEN 'Đã hoàn'
--     WHEN Order_Status = 'Shipped' THEN 'Đang giao'
--     WHEN Order_Status = 'AWAITING_COLLECTION' THEN 'Đang giao'
--     WHEN Order_Status = 'AWAITING_SHIPMENT' THEN 'Đang giao'
--     WHEN Order_Status = 'Canceled' THEN 'Đã hủy'
--     WHEN Order_Status = 'COMPLETED' THEN 'Đã giao thành công'
--     WHEN Order_Status = 'UNPAID' THEN 'Đăng đơn'
--     WHEN Order_Status = 'IN_TRANSIT' THEN 'Đang giao'
--     ELSE 'Khác'
-- END AS status,
-- FROM OrderData
-- ORDER BY Order_ID, SKU_ID
-- ),
-- a as(
-- SELECT
--     ord.*,
--     trans.order_statement_time,
--     trans.order_adjustment_id,
--     trans.adjustment_id,
--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.total_settlement_amount
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.total_settlement_amount, 0)
--     end as total_settlement_amount,
--     --  as total_settlement_amount,
    
--     -- COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_cost_amount, 0) AS phi_van_chuyen_thuc_te,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.platform_shipping_fee_discount, 0) AS phi_van_chuyen_tro_gia_tu_san,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.transaction_fee, 0) AS phi_thanh_toan,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.tiktok_shop_commission_fee, 0) AS phi_hoa_hong_shop,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_commission, 0) AS phi_hoa_hong_tiep_thi_lien_ket,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_shop_ads_commission, 0) AS phi_hoa_hong_quang_cao_cua_hang,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.sfp_service_fee, 0) AS phi_dich_vu,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) AS phi_ship,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.voucher_xtra_service_fee, 0) as phi_xtra,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vat_amount, 0) as thue_gtgt,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.pit_amount, 0) as thue_tncn,
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_partner_commission, 0) as affiliate_partner_commission,
--     0 as voucher_from_seller,
--     0 as phi_co_dinh,
--     gia_san_pham_goc_total - seller_tro_gia - san_tro_gia -  COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) AS tien_khach_hang_thanh_toan,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.tiktok_shop_commission_fee + trans.shipping_cost_amount +trans.affiliate_commission +trans.affiliate_shop_ads_commission +trans.sfp_service_fee +trans.voucher_xtra_service_fee + trans.vat_amount + trans.pit_amount
--         else    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.tiktok_shop_commission_fee, 0)+
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_cost_amount, 0)+
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_commission, 0)+
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_shop_ads_commission, 0)+
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_partner_commission, 0)+
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.sfp_service_fee, 0)+
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.voucher_xtra_service_fee, 0)+
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vat_amount, 0) +
--                 COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.pit_amount, 0) 
--     end as tong_phi_san,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.vat_amount + trans.pit_amount
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vat_amount, 0) +
--              COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.pit_amount, 0) 
--     end as tax,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.shipping_cost_amount
--         else   COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_cost_amount, 0)
--     end as phi_van_chuyen_thuc_te,
   

-- --  AS phi_van_chuyen_thuc_te,
--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.actual_shipping_fee + trans.platform_shipping_fee_discount + trans.customer_shipping_fee + trans.actual_return_shipping_fee + trans.refunded_customer_shipping_fee_amount +trans.failed_delivery_subsidy_amount
--         else  COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_shipping_fee, 0) +
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.platform_shipping_fee_discount, 0) +
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) +
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_return_shipping_fee, 0) +
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.refunded_customer_shipping_fee_amount, 0) +
--     COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.failed_delivery_subsidy_amount, 0)
--     end as seller_shipping_fee,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.actual_shipping_fee
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_shipping_fee, 0)
--     end as actual_shipping_fee,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.platform_shipping_fee_discount
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.platform_shipping_fee_discount, 0)
--     end as platform_shipping_fee_discount,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.customer_shipping_fee
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0)
--     end as customer_shipping_fee,


--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.actual_return_shipping_fee
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_return_shipping_fee, 0)
--     end as actual_return_shipping_fee,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.refunded_customer_shipping_fee_amount
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.refunded_customer_shipping_fee_amount, 0)
--     end as refunded_customer_shipping_fee_amount,

--     case
--         when order_type = "ZERO_LOTTERY"
--         then trans.failed_delivery_subsidy_amount
--         else COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.failed_delivery_subsidy_amount, 0)
--     end as failed_delivery_subsidy_amount,


-- FROM orderLine ord
-- LEFT JOIN OrderTotal total ON ord.brand = total.brand AND ord.ma_don_hang = total.Order_ID
-- LEFT JOIN {{ref("t2_tiktok_brand_statement_transaction_order_tot")}} trans ON ord.brand = trans.brand AND ord.ma_don_hang = trans.order_adjustment_id
-- -- where trans.type <>"LOGISTICS_REIMBURSEMENT"
-- )

-- select * from a -- where order_adjustment_id is not null -- and date(order_statement_time) = "2025-05-31" and brand = "Chaching"