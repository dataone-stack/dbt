SELECT 
  brand,
  order_id AS Order_ID,
  order_status AS Order_Status,
  NULL AS Order_Substatus, -- không có trong JSON
  cancel_reason AS Cancelation_Return_Type,
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.sku_type') AS Normal_or_Preorder,
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.sku_id') AS SKU_ID,
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.seller_sku') AS Seller_SKU,
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.product_name') AS Product_Name,
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.sku_name') AS Variation,
  ARRAY_LENGTH(line_items) AS Quantity, 
  NULL AS Sku_Quantity_of_Return, -- không có
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.original_price') AS SKU_Unit_Original_Price,
  JSON_EXTRACT_SCALAR(payment, '$.original_total_product_price') AS SKU_Subtotal_Before_Discount,
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.platform_discount') AS SKU_Platform_Discount,
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.seller_discount') AS SKU_Seller_Discount,
  JSON_EXTRACT_SCALAR(payment, '$.sub_total') AS SKU_Subtotal_After_Discount,
  JSON_EXTRACT_SCALAR(payment, '$.shipping_fee') AS Shipping_Fee_After_Discount,
  JSON_EXTRACT_SCALAR(payment, '$.original_shipping_fee') AS Original_Shipping_Fee,
  JSON_EXTRACT_SCALAR(payment, '$.shipping_fee_seller_discount') AS Shipping_Fee_Seller_Discount,
  JSON_EXTRACT_SCALAR(payment, '$.shipping_fee_platform_discount') AS Shipping_Fee_Platform_Discount,
  JSON_EXTRACT_SCALAR(payment, '$.platform_discount') AS Payment_Platform_Discount,
  JSON_EXTRACT_SCALAR(payment, '$.tax') AS Taxes,
  JSON_EXTRACT_SCALAR(payment, '$.total_amount') AS Order_Amount,
  NULL AS Order_Refund_Amount, -- không có
  DATETIME_ADD(create_time, INTERVAL 7 HOUR) AS Created_Time,
  paid_time AS Paid_Time,
  rts_time AS RTS_Time,
  collection_time AS Shipped_Time,
  delivery_time AS Delivered_Time,
  cancel_time AS Cancelled_Time,
  cancellation_initiator AS Cancel_By,
  cancel_reason AS Cancel_Reason,
  fulfillment_type AS Fulfillment_Type,
  NULL AS Warehouse_Name, -- không có
  tracking_number AS Tracking_ID,
  delivery_option_name AS Delivery_Option,
  shipping_provider AS Shipping_Provider_Name,
  buyer_message AS Buyer_Message,
  buyer_email AS Buyer_Username,
  JSON_EXTRACT_SCALAR(recipient_address, '$.name') AS Recipient,
  JSON_EXTRACT_SCALAR(recipient_address, '$.phone_number') AS Phone_Number,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(CAST(JSON_EXTRACT_ARRAY(recipient_address, '$.district_info') AS ARRAY<STRING>)) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L0') AS Country,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(CAST(JSON_EXTRACT_ARRAY(recipient_address, '$.district_info') AS ARRAY<STRING>)) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L1') AS Province,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(CAST(JSON_EXTRACT_ARRAY(recipient_address, '$.district_info') AS ARRAY<STRING>)) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L2') AS District,
  (SELECT JSON_EXTRACT_SCALAR(d, '$.address_name')
   FROM UNNEST(CAST(JSON_EXTRACT_ARRAY(recipient_address, '$.district_info') AS ARRAY<STRING>)) d
   WHERE JSON_EXTRACT_SCALAR(d, '$.address_level') = 'L3') AS Commune,
  JSON_EXTRACT_SCALAR(recipient_address, '$.address_detail') AS Detail_Address,
  JSON_EXTRACT_SCALAR(recipient_address, '$.address_line2') AS Additional_Address_Information,
  payment_method_name AS Payment_Method,
  NULL AS Weight_kg, -- không có
  NULL AS Product_Category, -- không có
  JSON_EXTRACT_SCALAR(line_items[OFFSET(0)], '$.package_id') AS Package_ID,
  seller_note AS Seller_Note,
  NULL AS Checked_Status, -- không có
  NULL AS Checked_Marked_by -- không có 
FROM {{ ref('t1_tiktok_order_tot')}}