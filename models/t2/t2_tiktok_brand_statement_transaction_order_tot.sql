with transactions as(
SELECT
    brand AS `brand`,
    company,
    shop,
    case
    when order_id is null and adjustment_id is not null
    then adjustment_order_id
    else order_id
    end AS `order_adjustment_id`,

    case
    when order_id is null and adjustment_id is not null
    then adjustment_id
    else order_id
    end AS `adjustment_id`,


    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(order_create_time)) AS `order_created_time`,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(statement_create_time)) AS `order_statement_time`,
    statement_currency AS `currency`,
    shipping_cost_amount as shipping_cost_amount,
    type,
    SAFE_CAST(settlement_amount AS FLOAT64) AS `total_settlement_amount`,
    SAFE_CAST(revenue_amount AS FLOAT64) AS `total_revenue`,
    (SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.subtotal_before_discount_amount') AS FLOAT64) + 
    SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.seller_discount_amount') AS FLOAT64)) AS `subtotal_after_seller_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.subtotal_before_discount_amount') AS FLOAT64) AS `subtotal_before_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.seller_discount_amount') AS FLOAT64) AS `seller_discounts`,
    (SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.refund_subtotal_before_discount_amount') AS FLOAT64) + 
    SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.seller_discount_refund_amount') AS FLOAT64)) AS `refund_subtotal_after_seller_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.refund_subtotal_before_discount_amount') AS FLOAT64) AS `refund_subtotal_before_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(revenue_breakdown, '$.seller_discount_refund_amount') AS FLOAT64) AS `refund_of_seller_discounts`,
    SAFE_CAST(fee_tax_amount AS FLOAT64) AS `total_fees`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.transaction_fee_amount') AS FLOAT64) AS `transaction_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.fbm_shipping_cost_amount') AS FLOAT64) AS `seller_shipping_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(shipping_cost_breakdown, '$.actual_shipping_fee_amount') AS FLOAT64) AS `actual_shipping_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(shipping_cost_breakdown, '$.supplementary_component.refunded_customer_shipping_fee_amount') AS FLOAT64) AS `refunded_customer_shipping_fee_amount`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(shipping_cost_breakdown, '$.supplementary_component.platform_shipping_fee_discount_amount') AS FLOAT64) AS `platform_shipping_fee_discount`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(shipping_cost_breakdown, '$.customer_paid_shipping_fee_amount') AS FLOAT64) AS `customer_shipping_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.retail_delivery_fee_refund_amount') AS FLOAT64) AS `refund_customer_shipping_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.failed_delivery_subsidy_amount') AS FLOAT64) AS `failed_delivery_subsidy_amount`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(shipping_cost_breakdown, '$.return_shipping_fee_amount') AS FLOAT64) AS `actual_return_shipping_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.platform_commission_amount') AS FLOAT64) AS `tiktok_shop_commission_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(shipping_cost_breakdown, '$.supplementary_component.shipping_fee_subsidy_amount') AS FLOAT64) AS `shipping_fee_subsidy`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.affiliate_commission_amount') AS FLOAT64) AS `affiliate_commission`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.affiliate_commission_amount_before_pit') AS FLOAT64) AS `affiliate_commission_before_pit`,
    0.0 AS `personal_income_tax_withheld_from_affiliate_commission`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.affiliate_ads_commission_amount') AS FLOAT64) AS `affiliate_shop_ads_commission`,
    0.0 AS `affiliate_shop_ads_commission_before_pit`,
    0.0 AS `personal_income_tax_withheld_from_affiliate_shop_ads_commission`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.affiliate_partner_commission_amount') AS FLOAT64) AS `affiliate_partner_commission`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.sfp_service_fee_amount') AS FLOAT64) AS `sfp_service_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.live_specials_fee_amount') AS FLOAT64) AS `live_specials_service_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.voucher_xtra_service_fee_amount') AS FLOAT64) AS `voucher_xtra_service_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.flash_sales_service_fee_amount') AS FLOAT64) AS `flash_sale_service_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.bonus_cashback_service_fee_amount') AS FLOAT64) AS `bonus_cashback_service_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.vn_fix_infrastructure_fee') AS FLOAT64) AS `vn_fix_infrastructure_fee`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.fee.shipping_fee_guarantee_service_fee') AS FLOAT64) AS `shipping_fee_guarantee_service_fee`,
    
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.tax.vat_amount') AS FLOAT64) AS `vat_withheld_by_tiktok_shop`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(fee_tax_breakdown, '$.tax.pit_amount') AS FLOAT64) AS `pit_withheld_by_tiktok_shop`,
    SAFE_CAST(adjustment_amount AS FLOAT64) AS `adjustment_amount`,
    order_id AS `related_order_id`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.customer_payment_amount') AS FLOAT64) AS `customer_payment`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.customer_refund_amount') AS FLOAT64) AS `customer_refund`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.seller_cofunded_discount_amount') AS FLOAT64) AS `seller_co_funded_voucher_discount`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.seller_cofunded_discount_refund_amount') AS FLOAT64) AS `refund_of_seller_co_funded_voucher_discount`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.platform_discount_amount') AS FLOAT64) AS `platform_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.platform_discount_refund_amount') AS FLOAT64) AS `refund_of_platform_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.platform_cofunded_discount_amount') AS FLOAT64) AS `platform_co_funded_voucher_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(supplementary_component, '$.platform_cofunded_discount_refund_amount') AS FLOAT64) AS `refund_of_platform_co_funded_voucher_discounts`,
    SAFE_CAST(JSON_EXTRACT_SCALAR(shipping_cost_breakdown, '$.supplementary_component.seller_shipping_fee_discount_amount') AS FLOAT64) AS `seller_shipping_fee_discount`,
    NULL AS `estimated_package_weight`,
    NULL AS `actual_package_weight`,





FROM {{ref("t1_tiktok_brand_statement_transaction_order_tot")}}  WHERE DATE(TIMESTAMP(order_create_time)) >= '2024-03-01' 
)
,a as(
SELECT 
    brand,
    shop,
    company,
    datetime_add(safe_cast(order_statement_time as datetime), INTERVAL 7 HOUR) as order_statement_time,
    order_adjustment_id,
    adjustment_id,
    currency,
    type,
    SUM(COALESCE(total_settlement_amount, 0)) as total_settlement_amount,
    SUM(COALESCE(total_revenue,0)) as total_revenue,
    SUM(COALESCE(actual_shipping_fee, 0)) AS actual_shipping_fee,
    SUM(COALESCE(platform_shipping_fee_discount, 0)) AS platform_shipping_fee_discount,
    SUM(COALESCE(transaction_fee, 0)) AS transaction_fee,
    SUM(COALESCE(customer_shipping_fee, 0)) AS customer_shipping_fee,
    
    SUM(COALESCE(tiktok_shop_commission_fee, 0)) AS tiktok_shop_commission_fee,
    SUM(COALESCE(shipping_cost_amount, 0)) AS shipping_cost_amount,
    SUM(COALESCE(affiliate_commission, 0)) AS affiliate_commission,
    SUM(COALESCE(affiliate_shop_ads_commission, 0)) AS affiliate_shop_ads_commission,
    SUM(COALESCE(affiliate_partner_commission, 0)) AS affiliate_partner_commission,
    SUM(COALESCE(sfp_service_fee, 0)) AS sfp_service_fee,
    SUM(COALESCE(voucher_xtra_service_fee, 0)) AS voucher_xtra_service_fee,
    SUM(COALESCE(vat_withheld_by_tiktok_shop, 0)) AS vat_amount,
    SUM(COALESCE(pit_withheld_by_tiktok_shop, 0)) AS pit_amount,
    SUM(COALESCE(actual_return_shipping_fee, 0)) AS actual_return_shipping_fee,
    SUM(COALESCE(failed_delivery_subsidy_amount, 0)) AS failed_delivery_subsidy_amount,
    SUM(COALESCE(customer_refund, 0)) AS customer_refund,
    SUM(COALESCE(refunded_customer_shipping_fee_amount, 0)) AS refunded_customer_shipping_fee_amount,
    SUM(COALESCE(vn_fix_infrastructure_fee, 0)) AS vn_fix_infrastructure_fee,
    SUM(COALESCE(shipping_fee_guarantee_service_fee, 0)) AS shipping_fee_guarantee_service_fee,
    SUM(COALESCE(refund_subtotal_before_discounts, 0)) AS refund_subtotal_before_discounts,
    SUM(COALESCE(refund_of_seller_discounts, 0)) AS refund_of_seller_discounts


FROM transactions
GROUP BY 
    brand,
    shop,
    company,
    order_statement_time,
    order_adjustment_id,
    adjustment_id,
    currency,
    type
)


select * from a-- where date(order_statement_time)-- between "2025-07-31" and "2025-08-28" and shop = "ume_beauty_vietnam_tt"