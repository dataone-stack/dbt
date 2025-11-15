WITH LineItems AS (
  SELECT
    o.brand,
    mapping.brand_lv1,
    -- mapping.company_lv1,
    o.order_id,
    o.company,
    o.shop,
    o.shop_id,
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
    o.shop_id,
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
    li.shop_id,
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
    o.shop_id,
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
    cost_price.cost_price * -1 as gia_von,
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
    shop_id,
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
        WHEN is_gift = TRUE THEN 0
        ELSE Gia_Ban_Daily * Quantity
    END AS gia_ban_daily_total,


    CASE
        WHEN line_type = 'return_line' THEN (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) * -1
        WHEN Order_Status = 'Canceled' THEN 0
        WHEN is_gift = TRUE THEN 0
        ELSE (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))
    END AS tien_chiet_khau_sp,


    CASE
        WHEN line_type = 'return_line' THEN ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) * -1
        WHEN Order_Status = 'Canceled' THEN 0
        ELSE ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))
    END AS doanh_thu_ke_toan,


    (COALESCE(gia_von, 0) * COALESCE(Quantity, 0)) as gia_von_total,
    COALESCE(gia_von, 0) as gia_von,

    CASE
      WHEN is_gift = TRUE THEN "Quà tặng"
      ELSE "Hàng bán"
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
    ord.brand,
    ord.brand_lv1,
    ord.shop,
    ord.shop_id,
    ord.company,
    ord.ma_don_hang,
    ord.Order_Status,
    ord.Order_Substatus,
    ord.Cancelation_Return_Type,
    ord.Normal_or_Preorder,
    ord.SKU_ID,
    ord.sku_code,
    ord.ten_san_pham,
    ord.Variation,
    ord.so_luong,
    ord.Sku_Quantity_of_Return,
    ord.SKU_Unit_Original_Price,
    ord.SKU_Subtotal_Before_Discount,
    ord.san_tro_gia,
    case
    when trans.total_revenue = 0
    then 0
    else ord.seller_tro_gia
    end as seller_tro_gia,

    ord.giam_gia_seller,
    ord.giam_gia_san,
    ord.SKU_Subtotal_After_Discount,
    ord.Shipping_Fee_After_Discount,
    ord.Original_Shipping_Fee,
    ord.Shipping_Fee_Seller_Discount,
    ord.Shipping_Fee_Platform_Discount,
    ord.Payment_Platform_Discount,
    ord.Taxes,
    ord.Order_Amount,
    ord.Order_Refund_Amount,
    ord.ngay_tao_don,
    ord.Paid_Time,
    ord.RTS_Time,
    ord.Shipped_Time,
    ord.Delivered_Time,
    ord.Cancelled_Time,
    ord.Cancel_By,
    ord.Cancel_Reason,
    ord.Fulfillment_Type,
    ord.Warehouse_Name,
    ord.Tracking_ID,
    ord.Delivery_Option,
    ord.Shipping_Provider_Name,
    ord.Buyer_Message,
    ord.Buyer_Username,
    ord.Recipient,
    ord.Phone_Number,
    ord.Country,
    ord.Province,
    ord.District,
    ord.Commune,
    ord.Detail_Address,
    ord.Additional_Address_Information,
    ord.Payment_Method,
    ord.Weight_kg,
    ord.Product_Category,
    ord.Package_ID,
    ord.Seller_Note,
    ord.Checked_Status,
    ord.Checked_Marked_by,
    ord.gia_ban_daily,
    ord.gia_san_pham_goc,

    case 
    when trans.total_revenue = 0 
    then 0
    else ord.gia_san_pham_goc_total
    end as gia_san_pham_goc_total,

    ord.tien_sp_sau_tro_gia,
    ord.gia_ban_daily_total,
    ord.tien_chiet_khau_sp,
    
    -- CASE
    --   WHEN COALESCE(trans.total_revenue, 0) = 0 THEN 0
    --   ELSE ord.gia_ban_daily_total
    -- END AS gia_ban_daily_total,

    -- CASE
    --   WHEN COALESCE(trans.total_revenue, 0) = 0 THEN 0
    --   ELSE ord.tien_chiet_khau_sp
    -- END AS tien_chiet_khau_sp,
    -- Thêm logic điều kiện cho doanh_thu_ke_toan
    CASE
      WHEN COALESCE(trans.total_revenue, 0) = 0 THEN 0
      ELSE ord.doanh_thu_ke_toan
    END AS doanh_thu_ke_toan,
    
    ord.gia_von_total,
    ord.gia_von,
    ord.promotion_type,
    ord.order_type,
    ord.line_type,
    ord.status,
    
    trans.order_statement_time,
    trans.order_adjustment_id,
    trans.adjustment_id,
    
    -- Tiếp tục với các tính toán khác
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
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vn_fix_infrastructure_fee, 0) as phi_xu_ly_don_hang,
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_fee_guarantee_service_fee, 0)  as phi_dich_vu_sfr,
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_fee_guarantee_reimbursement, 0) as hoan_phi_sfr,
    
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.refund_subtotal_before_discounts, 0)  as tong_phu_hoan_tien_truoc_giam_gia_cua_nguoi_ban,
    COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.refund_of_seller_discounts, 0)  as khoan_hoan_tien_giam_gia_cua_ban,
    0 as voucher_from_seller,
    0 as phi_co_dinh,
    
    -- Tiền khách hàng thanh toán
    ord.gia_san_pham_goc_total - ord.seller_tro_gia - ord.san_tro_gia - COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) AS tien_khach_hang_thanh_toan,


    -- Tổng phí sàn
    CASE
        WHEN ord.order_type = "ZERO_LOTTERY"
        THEN 
        trans.transaction_fee + trans.tiktok_shop_commission_fee + trans.actual_shipping_fee + trans.platform_shipping_fee_discount + trans.customer_shipping_fee + trans.actual_return_shipping_fee + trans.refunded_customer_shipping_fee_amount + trans.failed_delivery_subsidy_amount
         + trans.affiliate_commission + trans.affiliate_shop_ads_commission + trans.affiliate_partner_commission
        + trans.sfp_service_fee + trans.voucher_xtra_service_fee + trans.vat_amount + trans.pit_amount + trans.vn_fix_infrastructure_fee + trans.shipping_fee_guarantee_service_fee

        ELSE COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.tiktok_shop_commission_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.transaction_fee, 0) +
             
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_shipping_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.platform_shipping_fee_discount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.customer_shipping_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.actual_return_shipping_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.refunded_customer_shipping_fee_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.failed_delivery_subsidy_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_fee_guarantee_reimbursement, 0)+

             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_commission, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_shop_ads_commission, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.affiliate_partner_commission, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.sfp_service_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.voucher_xtra_service_fee, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vat_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.pit_amount, 0) +
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.vn_fix_infrastructure_fee, 0)+
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_fee_guarantee_service_fee, 0)
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
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.failed_delivery_subsidy_amount, 0)+
             COALESCE((ord.SKU_Subtotal_After_Discount / NULLIF(total.tong_tien_sau_giam_gia, 0)) * trans.shipping_fee_guarantee_reimbursement, 0)
             
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
  LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t2_tiktok_brand_statement_transaction_order_tot` trans 
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

, a as (
SELECT *,
case
  when hoan_phi_sfr > 0 and status = 'Đã hoàn'
  then 'Đơn hoàn đã hoàn phí vận chuyển'
  when hoan_phi_sfr = 0 and status = 'Đã hoàn'
  then 'Đơn hoàn chưa hoàn phí vận chuyển'
  else '-'
end as check_hoan_phi_sfr
FROM final_result where order_statement_time is not null)

select * from a 