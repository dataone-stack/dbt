SELECT
  brand,
  Order_ID,
  Order_Status,
  Order_Substatus,
  
  -- Tổng hợp thông tin sản phẩm
  COUNT(DISTINCT SKU_ID) AS Total_SKU_Count,
  SUM(Quantity) AS Total_Quantity,
  SUM(Sku_Quantity_of_Return) AS Total_Quantity_of_Return,
  STRING_AGG(DISTINCT Product_Name, '; ' ORDER BY Product_Name) AS Product_Names,
  STRING_AGG(DISTINCT Seller_SKU, '; ' ORDER BY Seller_SKU) AS Seller_SKUs,
  STRING_AGG(DISTINCT Variation, '; ' ORDER BY Variation) AS Variations,
  STRING_AGG(DISTINCT Normal_or_Preorder, '; ') AS Order_Types,
  STRING_AGG(DISTINCT Cancelation_Return_Type, '; ') AS Cancelation_Return_Types,
  
  -- Tổng hợp giá trị đơn hàng
  SUM(SKU_Subtotal_Before_Discount) AS Total_Subtotal_Before_Discount,
  SUM(SKU_Platform_Discount) AS Total_SKU_Platform_Discount,
  SUM(SKU_Seller_Discount) AS Total_SKU_Seller_Discount,
  SUM(SKU_Subtotal_After_Discount) AS Total_Subtotal_After_Discount,
  SUM(COALESCE(Order_Refund_Amount, 0)) AS Total_Refund_Amount,
  
  -- Phí vận chuyển và thuế (lấy giá trị đầu tiên vì giống nhau trong cùng order)
  ANY_VALUE(Shipping_Fee_After_Discount) AS Shipping_Fee_After_Discount,
  ANY_VALUE(Original_Shipping_Fee) AS Original_Shipping_Fee,
  ANY_VALUE(Shipping_Fee_Seller_Discount) AS Shipping_Fee_Seller_Discount,
  ANY_VALUE(Shipping_Fee_Platform_Discount) AS Shipping_Fee_Platform_Discount,
  ANY_VALUE(Payment_Platform_Discount) AS Payment_Platform_Discount,
  ANY_VALUE(Taxes) AS Taxes,
  ANY_VALUE(Order_Amount) AS Order_Amount,
  
  -- Thời gian (lấy giá trị đầu tiên)
  ANY_VALUE(Created_Time) AS Created_Time,
  ANY_VALUE(Paid_Time) AS Paid_Time,
  ANY_VALUE(RTS_Time) AS RTS_Time,
  ANY_VALUE(Shipped_Time) AS Shipped_Time,
  ANY_VALUE(Delivered_Time) AS Delivered_Time,
  ANY_VALUE(Cancelled_Time) AS Cancelled_Time,
  
  -- Thông tin hủy đơn
  ANY_VALUE(Cancel_By) AS Cancel_By,
  STRING_AGG(DISTINCT Cancel_Reason, '; ') AS Cancel_Reasons,
  
  -- Thông tin vận chuyển
  ANY_VALUE(Fulfillment_Type) AS Fulfillment_Type,
  ANY_VALUE(Warehouse_Name) AS Warehouse_Name,
  ANY_VALUE(Tracking_ID) AS Tracking_ID,
  ANY_VALUE(Delivery_Option) AS Delivery_Option,
  ANY_VALUE(Shipping_Provider_Name) AS Shipping_Provider_Name,
  
  -- Thông tin khách hàng
  ANY_VALUE(Buyer_Message) AS Buyer_Message,
  ANY_VALUE(Buyer_Username) AS Buyer_Username,
  ANY_VALUE(Recipient) AS Recipient,
  ANY_VALUE(Phone_Number) AS Phone_Number,
  
  -- Địa chỉ
  ANY_VALUE(Country) AS Country,
  ANY_VALUE(Province) AS Province,
  ANY_VALUE(District) AS District,
  ANY_VALUE(Commune) AS Commune,
  ANY_VALUE(Detail_Address) AS Detail_Address,
  ANY_VALUE(Additional_Address_Information) AS Additional_Address_Information,
  
  -- Thông tin bổ sung
  CASE 
    WHEN SUM(Sku_Quantity_of_Return) > 0 THEN TRUE 
    ELSE FALSE 
  END AS Has_Returns,
  ROUND(SUM(SKU_Subtotal_After_Discount) + ANY_VALUE(Shipping_Fee_After_Discount) + ANY_VALUE(Taxes), 2) AS Calculated_Total_Amount

FROM {{ref('t2_tiktok_order_line_toa')}}
GROUP BY 
  brand,
  Order_ID,
  Order_Status,
  Order_Substatus
ORDER BY 
  brand,
  Order_ID
