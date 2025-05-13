SELECT
  o.brand,
  o.order_id AS Order_ID,
  CASE o.order_status
    WHEN 'CANCELLED' THEN 'Canceled'
    ELSE o.order_status
  END AS Order_Status,
  CASE o.order_status
    WHEN 'CANCELLED' THEN 'Canceled'
    ELSE NULL
  END AS Order_Substatus,
  CASE o.order_status
    WHEN 'CANCELLED' THEN 'Cancel'
    ELSE o.cancel_reason
  END AS Cancelation_Return_Type,
  JSON_EXTRACT_SCALAR(o.line_items[OFFSET(0)], '$.sku_type') AS Normal_or_Preorder,
  JSON_EXTRACT_SCALAR(o.line_items[OFFSET(0)], '$.sku_id') AS SKU_ID,
  JSON_EXTRACT_SCALAR(o.line_items[OFFSET(0)], '$.seller_sku') AS Seller_SKU,
  JSON_EXTRACT_SCALAR(o.line_items[OFFSET(0)], '$.product_name') AS Product_Name,
  JSON_EXTRACT_SCALAR(o.line_items[OFFSET(0)], '$.sku_name') AS Variation,
  o.item_count AS Quantity,
  o.item_count AS Sku_Quantity_of_Return, -- Giả định tất cả SKU được hoàn khi hủy
  CAST(JSON_EXTRACT_SCALAR(o.line_items[OFFSET(0)], '$.original_price') AS FLOAT64) AS SKU_Unit_Original_Price,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.original_total_product_price') AS FLOAT64) AS SKU_Subtotal_Before_Discount,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.platform_discount') AS FLOAT64) AS SKU_Platform_Discount, -- Tổng discount cho toàn đơn
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.seller_discount') AS FLOAT64) AS SKU_Seller_Discount, -- Tổng discount cho toàn đơn
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.sub_total') AS FLOAT64) AS SKU_Subtotal_After_Discount,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.shipping_fee') AS FLOAT64) AS Shipping_Fee_After_Discount,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.original_shipping_fee') AS FLOAT64) AS Original_Shipping_Fee,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.shipping_fee_seller_discount') AS FLOAT64) AS Shipping_Fee_Seller_Discount,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.shipping_fee_platform_discount') AS FLOAT64) AS Shipping_Fee_Platform_Discount,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.platform_discount') AS FLOAT64) AS Payment_Platform_Discount,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.tax') AS FLOAT64) AS Taxes,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.total_amount') AS FLOAT64) AS Order_Amount,
  CAST(JSON_EXTRACT_SCALAR(o.payment, '$.total_amount') AS FLOAT64) AS Order_Refund_Amount, -- Hoàn toàn bộ khi hủy
  DATETIME_ADD(o.create_time, INTERVAL 7 HOUR) AS Created_Time, -- UTC+7
  o.paid_time AS Paid_Time,
  o.rts_time AS RTS_Time,
  o.collection_time AS Shipped_Time,
  o.delivery_time AS Delivered_Time,
  DATETIME_ADD(o.cancel_time, INTERVAL 7 HOUR) AS Cancelled_Time, -- UTC+7
  CASE o.cancellation_initiator
    WHEN 'BUYER' THEN 'User'
    ELSE o.cancellation_initiator
  END AS Cancel_By,
  CASE o.cancel_reason
    WHEN 'Không còn nhu cầu' THEN 'No longer needed'
    ELSE o.cancel_reason
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
  o.buyer_email AS Buyer_Username, -- Dùng buyer_email, có thể cần bảng người dùng
  JSON_EXTRACT_SCALAR(o.recipient_address, '$.name') AS Recipient,
  JSON_EXTRACT_SCALAR(o.recipient_address, '$.phone_number') AS Phone_Number,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(JSON_EXTRACT_ARRAY(o.recipient_address, '$.district_info')) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L0') AS Country,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(JSON_EXTRACT_ARRAY(o.recipient_address, '$.district_info')) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L1') AS Province,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(JSON_EXTRACT_ARRAY(o.recipient_address, '$.district_info')) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L2') AS District,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(JSON_EXTRACT_ARRAY(o.recipient_address, '$.district_info')) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L3') AS Commune,
  JSON_EXTRACT_SCALAR(o.recipient_address, '$.address_detail') AS Detail_Address,
  JSON_EXTRACT_SCALAR(o.recipient_address, '$.address_line2') AS Additional_Address_Information,
  CASE o.payment_method_name
    WHEN 'Cash on delivery' THEN 'Thanh toán khi giao hàng'
    ELSE o.payment_method_name
  END AS Payment_Method,
  NULL AS Weight_kg, -- Không có trong JSON
  NULL AS Product_Category, -- Không có trong JSON
  JSON_EXTRACT_SCALAR(pck, '$.id') AS Package_ID,
  o.seller_note AS Seller_Note,
  'Unchecked' AS Checked_Status,
  NULL AS Checked_Marked_by
FROM {{ ref('t1_tiktok_order_tot')}} o
CROSS JOIN UNNEST(o.packages) AS pck