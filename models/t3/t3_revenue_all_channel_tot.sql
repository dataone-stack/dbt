SELECT brand, CAST(ngay_hoan_thanh_nhan_don_hang AS TIMESTAMP) as date_create, ref_code as order_id, trang_thai as status, sku as sku_code, ten_san_pham as product_name, doanh_thu_don_hang as total_amount, CAST(ngay_tao AS TIMESTAMP) as date_create_order, 'Facebook' AS channel,
FROM {{ ref('t2_facebook_order_lines_tot') }}
UNION ALL
SELECT brand, CAST(ngay_hoan_thanh_thanh_toan AS TIMESTAMP) as date_create, order_id,"" as status, ma_san_pham as sku_code, ten_san_pham as product_name, tong_tien_da_thanh_toan as total_amount, ngay_dat_hang as CAST(date_create_order AS TIMESTAMP), 'Shopee' AS channel,
FROM {{ ref('t2_shopee_order_lines_tot') }}
UNION ALL
SELECT brand, CAST(order_statement_time AS TIMESTAMP) as date_create, ma_don_hang as order_id, Order_Status as status, sku_code, ten_san_pham as product_name, total_settlement_amount as total_amount, ngay_tao_don as CAST(date_create_order AS TIMESTAMP), 'Tiktok' AS channel, 
FROM {{ ref('t2_tiktok_order_line_tot') }}