WITH LineItems AS (
  SELECT
    o.brand,
    o.order_id,
    o.company,
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
    sum(cost_price.cost_price) as cost_price
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_tiktok_order_tot` o
  CROSS JOIN UNNEST(o.line_items) AS li
  LEFT JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_bang_gia_san_pham` AS mapping
    ON JSON_VALUE(li, '$.seller_sku') = mapping.ma_sku
  left join `google_sheet.bang_gia_von` as cost_price on JSON_VALUE(li, '$.seller_sku') = cost_price.product_sku
  GROUP BY
    o.brand,
    o.company,
    o.order_id,
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
    mapping.gia_ban_daily
)
--select * from LineItems where Order_ID = 579785176962401968
,

ReturnLineItems AS (
  SELECT
    r.order_id,
    r.brand,
    JSON_VALUE(li, '$.sku_id') AS SKU_ID,
    COALESCE(CAST(JSON_VALUE(li, '$.quantity') AS INT64), 1) AS Sku_Quantity_of_Return,
    CAST(JSON_VALUE(r.refund_amount, '$.refund_total') AS FLOAT64) AS Order_Refund_Amount,
    CASE r.return_status 
        WHEN 'RETURN_OR_REFUND_REQUEST_COMPLETE' THEN 'return_refund'
        ELSE null
    END AS Cancelation_Return_Type
  FROM `crypto-arcade-453509-i8`.`dtm`.`t1_tiktok_order_return` r
  CROSS JOIN UNNEST(r.return_line_items) AS li
  left join  `crypto-arcade-453509-i8`.`dtm`.`t1_bang_gia_san_pham` AS mapping 
  on json_value(li,'$.seller_sku') = mapping.ma_sku
  where r.return_status = 'RETURN_OR_REFUND_REQUEST_COMPLETE'
)
--select * from ReturnLineItems where order_id = 579785176962401968
,

OrderData AS (
  SELECT
    li.brand,
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
    -- Map return info nếu có, ưu tiên Cancelation_Return_Type từ return
    COALESCE(r.Cancelation_Return_Type,
      CASE 
        WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.SKU_Display_Status = 'CANCELLED' THEN 'Cancel'
        ELSE NULL
      END) AS Cancelation_Return_Type,
    li.Normal_or_Preorder,
    li.is_gift,
    li.SKU_ID,
    li.Seller_SKU,
    li.Product_Name,
    li.Variation,
    li.Quantity,
    -- Nếu có return thì lấy số lượng return, không thì lấy theo cancel logic cũ
    COALESCE(r.Sku_Quantity_of_Return,
      CASE 
        WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.is_gift = FALSE AND li.SKU_Display_Status = 'CANCELLED' THEN li.Quantity
        ELSE 0
      END) AS Sku_Quantity_of_Return,
    case
        when li.is_gift = TRUE
        then 0
        else li.SKU_Unit_Original_Price
    end as SKU_Unit_Original_Price,
    li.SKU_Subtotal_Before_Discount,
    li.SKU_Platform_Discount,

    case
        when li.is_gift = TRUE
        then 0
        else li.SKU_Seller_Discount
    end as SKU_Seller_Discount,
    
    li.SKU_Subtotal_After_Discount,

    CASE
        when li.is_gift = TRUE
        then 0
        else li.Gia_Ban_Daily
    end as Gia_Ban_Daily,

    li.cost_price,

    CAST(JSON_VALUE(o.payment, '$.shipping_fee') AS FLOAT64) AS Shipping_Fee_After_Discount,
    CAST(JSON_VALUE(o.payment, '$.original_shipping_fee') AS FLOAT64) AS Original_Shipping_Fee,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee_seller_discount') AS FLOAT64) AS Shipping_Fee_Seller_Discount,
    CAST(JSON_VALUE(o.payment, '$.shipping_fee_platform_discount') AS FLOAT64) AS Shipping_Fee_Platform_Discount,
    0 AS Payment_Platform_Discount,
    CAST(JSON_VALUE(o.payment, '$.tax') AS FLOAT64) AS Taxes,
    CAST(JSON_VALUE(o.payment, '$.total_amount') AS FLOAT64) AS Order_Amount,
    -- Lấy tiền hoàn trả từ return nếu có, nếu không thì theo logic cũ
    COALESCE(r.Order_Refund_Amount,
      CASE 
        WHEN li.SKU_Cancel_Reason IS NOT NULL AND li.SKU_Display_Status = 'CANCELLED' THEN li.SKU_Refund_Amount
        ELSE NULL
      END) AS Order_Refund_Amount,
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
    NULL AS Product_Category,
    li.Package_ID,
    o.seller_note AS Seller_Note,
    'Unchecked' AS Checked_Status,
    NULL AS Checked_Marked_by
  FROM LineItems li
  JOIN `crypto-arcade-453509-i8`.`dtm`.`t1_tiktok_order_tot` o
    ON li.order_id = o.order_id
  LEFT JOIN ReturnLineItems r
    ON li.order_id = r.order_id
    AND li.SKU_ID = r.SKU_ID
    and li.brand = r.brand
  left join  `crypto-arcade-453509-i8`.`dtm`.`t1_bang_gia_san_pham` as mapping on
  li.Seller_SKU = mapping.ma_sku
)
--select * from OrderData where Order_ID = 579785176962401968
,

orderLine as(
SELECT
  brand,
  brand as brand_lv1,
  company,
  Order_ID as ma_don_hang,
  Order_Status,
  Order_Substatus,
  Cancelation_Return_Type,
  Normal_or_Preorder,
  SKU_ID ,
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
  Gia_Ban_Daily * Quantity AS gia_ban_daily_total,
  cost_price  * Quantity AS gia_von,
  COALESCE(SKU_Unit_Original_Price, 0) AS gia_san_pham_goc,
  COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0) AS gia_san_pham_goc_total,
  (COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0) AS tien_sp_sau_tro_gia,
  
  (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0)) AS tien_chiet_khau_sp,
  (COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(Gia_Ban_Daily, 0) * COALESCE(Quantity, 0)) - ((COALESCE(SKU_Unit_Original_Price, 0) * COALESCE(Quantity, 0)) - COALESCE(SKU_Seller_Discount, 0))) AS doanh_thu_ke_toan,
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
FROM OrderData
ORDER BY Order_ID, SKU_ID
)
-- select * from orderLine where ma_don_hang =580087776528139654 --brand is null and date(ngay_tao_don) between "2025-07-01" and "2025-09-03"
,

order_total AS (
    SELECT
        brand,
        brand as brand_lv1,
        company,
        ma_don_hang,
        Order_Status,
        ngay_tao_don,
        Shipped_Time,
        "" as status,
        SUM(gia_ban_daily_total) AS gia_ban_daily_total,
        SUM(doanh_thu_ke_toan) AS doanh_thu_ke_toan,
        SUM(doanh_thu_ke_toan) AS doanh_thu_ke_toan_v2,
        SUM(tien_chiet_khau_sp) AS tien_chiet_khau_sp,
        sum(gia_von) as gia_von
    FROM orderLine
    GROUP BY
        brand,
        brand_lv1,
        company,
        ma_don_hang,
        Order_Status,
        -- status,
        ngay_tao_don,
        Shipped_Time
)
-- select * from order_total where ma_don_hang = 579980156334802932

,

a AS (
    SELECT 
        ord.*,
        trans.total_settlement_amount,
        trans.total_revenue,
        trans.order_statement_time,
        order_adjustment_id,
        trans.shop,
        trans.actual_shipping_fee as phi_van_chuyen_thuc_te,
        
        -- (trans.transaction_fee + trans.tiktok_shop_commission_fee  + trans.affiliate_commission + trans.affiliate_shop_ads_commission + trans.sfp_service_fee + trans.customer_shipping_fee + trans.voucher_xtra_service_fee) as phu_phi
        trans.total_revenue - trans.total_settlement_amount as phu_phi
   FROM `dtm.t2_tiktok_brand_statement_transaction_order_tot` AS trans
    LEFT JOIN  order_total AS ord 
        ON ord.ma_don_hang = trans.order_adjustment_id
        -- AND ord.brand = trans.brand

)

--select ma_don_hang,status,sum(total_settlement_amount) from a where brand = 'LYB' and date(order_statement_time) between '2025-08-01' and '2025-08-31' group by ma_don_hang,status

select * from a  -- and ma_don_hang = 580087776528139654