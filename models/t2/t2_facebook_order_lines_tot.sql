WITH totalGiamGiaSp as(
    select or_code,brand,
    sum(CAST(JSON_VALUE(i, '$.discountValue') AS FLOAT64)) as total_dis
    from {{ref("t1_vietful_xuatkho_total")}}
    CROSS JOIN UNNEST(details) AS i
    group by or_code,brand
),
vietful_orderline AS (
  SELECT 
    ord.brand,
    ord.or_code AS ma_or,
    ord.partner_or_code AS ma_or_doi_tac,
    ord.status AS trang_thai,
    ord.warehouse_code AS kho,
    ord.sale_channel_code AS ma_kbh, 
    ord.created_date AS ngay_tao,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '41'
     LIMIT 1) AS ngay_bat_dau_xu_ly,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '42'
     LIMIT 1) AS ngay_lay_hang,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '60'
     LIMIT 1) AS ngay_dong_goi,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '63'
     LIMIT 1) AS ngay_ban_giao_van_chuyen,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '71'
     LIMIT 1) AS ngay_da_giao,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '20'
     LIMIT 1) AS ngay_huy_don,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '80'
     LIMIT 1) AS ngay_hoan_don,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '81'
     LIMIT 1) AS ngay_hoan_toi_kho,
    (SELECT AS VALUE JSON_VALUE(status, '$.statusDate')
     FROM UNNEST(ord.status_trackings) AS status
     WHERE JSON_VALUE(status, '$.statusCode') = '83'
     LIMIT 1) AS ngay_hoan_thanh_nhan_don_hang,
    prd.sku,
    prd.product_name AS ten_san_pham,
    COALESCE(
      CASE 
        WHEN ARRAY_LENGTH(prd.units) > 0 THEN SAFE_CAST(JSON_VALUE(prd.units[0], '$.length') AS float64)
        ELSE NULL
      END,
      0
    ) AS L,
    COALESCE(
      CASE 
        WHEN ARRAY_LENGTH(prd.units) > 0 THEN SAFE_CAST(JSON_VALUE(prd.units[0], '$.width') AS float64)
        ELSE NULL
      END,
      0
    ) AS W,
    COALESCE(
      CASE 
        WHEN ARRAY_LENGTH(prd.units) > 0 THEN SAFE_CAST(JSON_VALUE(prd.units[0], '$.height') AS float64)
        ELSE NULL
      END,
      0
    ) AS H,
    COALESCE(
      CASE 
        WHEN ARRAY_LENGTH(prd.units) > 0 THEN SAFE_CAST(JSON_VALUE(prd.units[0], '$.weight') AS float64)
        ELSE NULL
      END,
      0
    ) AS khoi_luong,
    CASE
      WHEN ARRAY_LENGTH(prd.product_bundles) = 0 
      THEN 'Bundle'
      ELSE 'Hàng lẻ'
    END AS loai_san_pham,
    COALESCE(   
      CASE 
        WHEN ARRAY_LENGTH(prd.categories) > 0 THEN JSON_VALUE(prd.categories[OFFSET(0)], '$.categoryName')
        ELSE NULL
      END,
      '-'
    ) AS danh_muc,
    COALESCE(
      CASE 
        WHEN ARRAY_LENGTH(prd.units) > 0 THEN JSON_VALUE(prd.units[OFFSET(0)], '$.unitName')
        ELSE NULL
      END,
      '-'
    ) AS don_vi_tinh,
    ord.note AS ghi_chu,
    ord.packing_note AS ghi_chu_don_hang,
    ord.shipping_service_name AS dich_vu_giao_hang,
    CASE
      WHEN ord.shipping_service_name = 'Standard' THEN 'Lấy hàng tại kho'
      ELSE 'Dịch vụ vận chuyển'
    END AS hinh_thuc_nhan_hang,
    JSON_VALUE(i, '$.partnerSKU') AS ma_sku_doi_tac,
    CAST(JSON_VALUE(i, '$.orderQty') AS INT64) AS so_luong_cua_don,
    CAST(JSON_VALUE(i, '$.packedQty') AS INT64) AS so_luong_dong_goi,
    CAST(JSON_VALUE(i, '$.price') AS FLOAT64) AS gia_ban_san_pham,
    CAST(JSON_VALUE(i, '$.discountValue') AS FLOAT64) AS giam_gia,
    CAST(JSON_VALUE(i, '$.orderQty') AS INT64) *
    (CAST(JSON_VALUE(i, '$.price') AS FLOAT64) - CAST(JSON_VALUE(i, '$.discountValue') AS FLOAT64)) AS thanh_tien,

     COALESCE(
      SAFE_DIVIDE(
        CAST(JSON_VALUE(i, '$.orderQty') AS INT64) *
        (CAST(JSON_VALUE(i, '$.price') AS FLOAT64) - CAST(JSON_VALUE(i, '$.discountValue') AS FLOAT64)),
        NULLIF(ord.total_amount, 0)
      ) * (ord.discount_amount - dis.total_dis ), 0) as giam_gia_don_hang,
    

    ord.tracking_code AS ma_van_don,
    ord.ref_code,
    COALESCE(
      CASE 
        WHEN ARRAY_LENGTH(ord.packages) > 0 THEN JSON_VALUE(ord.packages[OFFSET(0)], '$.packageNo')
        ELSE NULL
      END,
      '-'
    ) AS ma_kien_hang,
    ord.customer_name AS ten_nguoi_mua,
    ord.customer_phone_number AS sdt,
    ord.shipping_full_address AS dia_chi
  FROM {{ref("t1_vietful_xuatkho_total")}} AS ord
  CROSS JOIN UNNEST(ord.details) AS i
  LEFT JOIN {{ref("t1_vietful_product_total")}} AS prd
  ON JSON_VALUE(i, '$.sku') = prd.sku AND ord.brand = prd.brand
  left join totalGiamGiaSp as dis 
  on ord.brand = dis.brand and ord.or_code = dis.or_code
  WHERE ord.sale_channel_code = 'PANCAKE' and ord.status = 'Delivered'
)
SELECT
  brand,
  ngay_tao,
  '-' AS ngay_phat_sinh_don,
  kho,
  'CHA - HỘ KINH DOANH NGUYỄN THỊ NHUNG' AS doi_tac,
  ma_or,
  ma_or_doi_tac,
  ma_kbh,
  CONCAT(
    UPPER(SUBSTR(ma_kbh, 1, 1)),  
    LOWER(SUBSTR(ma_kbh, 2))   
  ) AS ten_kbh,
  'Đặt hàng' AS loai,
  trang_thai,
--   case
--     WHEN trang_thai = 'Delivered'
--     THEN 'Đã giao thành công'
--     WHEN LOWER(ghi_chu) LIKE '%ds%' OR LOWER(ghi_chu) LIKE '%đổi size%' OR LOWER(ghi_chu) like "%thu hồi%" or ghi_chu in ('Returned','OnReturn', 'ReturnReceived') 
--     THEN 'Đã hoàn'
--     WHEN trang_thai in ('New') 
--     THEN 'Đăng đơn'
--     when trang_thai in ('Delivering')
--     then 'Đang giao hàng'
--     when trang_thai in ('FailDelivery','Cancelled','Error')
--     then 'Đã hủy'
--     when trang_thai in ('Shipped')
--     then 'Đã bàn giao vận chuyển'
--     when trang_thai in ('ReadyToShip','TPLConfirmed')
--     then 'Sẫn sàng bàn giao'
--     when trang_thai in ('Processing','TPLTransit')
--     then 'Đang bàn giao vận chuyển'
--     when trang_thai in ('Delay')
--     then 'Hoãn lại'
--     else trang_thai
--   end as trang_thai,
  sku,
  ma_sku_doi_tac,
  so_luong_cua_don,
  so_luong_dong_goi,
  ten_san_pham,
  'Hàng quản lý theo số lượng' AS loai_luu_tru,
  CONCAT(
    CAST(L AS STRING), 'x',
    CAST(W AS STRING), 'x',
    CAST(H AS STRING)
  ) AS LWH,
  L,
  W,
  H,
  khoi_luong,
  loai_san_pham,
  '-' AS quy_cach,
  danh_muc,
  don_vi_tinh,
  ghi_chu,
  ghi_chu_don_hang,
  '-' AS ghi_chu_sp,
  hinh_thuc_nhan_hang,
  dich_vu_giao_hang,
  round(thanh_tien - giam_gia_don_hang) as cod,
  0 AS khai_gia,
  gia_ban_san_pham,
  giam_gia,
  round(giam_gia_don_hang) as giam_gia_don_hang ,
  thanh_tien,
  round(thanh_tien - giam_gia_don_hang) as doanh_thu_don_hang,
  0 AS phi_xu_ly_don_hang,
  0 AS phi_van_chuyen_dich_vu,
  'New' AS tinh_trang_hang_hoa,
  khoi_luong * so_luong_cua_don AS tong_khoi_luong,
  L * W * H * so_luong_cua_don AS tong_dung_tich,
  ma_kien_hang,
  ma_van_don,
  '-' AS ma_van_don_thu_hoi,
  '-' AS sla_xu_ly_kbh,
  ngay_bat_dau_xu_ly,
  '-' AS sla_dong_goi,
  ngay_lay_hang,
  ngay_dong_goi,
  ngay_ban_giao_van_chuyen,
  ngay_da_giao,
  ngay_huy_don,
  ngay_hoan_don,
  ngay_hoan_toi_kho,
  ngay_hoan_thanh_nhan_don_hang,
  '-' AS sla_don_vi_van_chuyen,
  ref_code,
  ten_nguoi_mua,
  sdt,
  dia_chi,
  'B2C' AS loai_hinh,
  '-' AS do_uu_tien,
  '-' AS tai_xe,
  '-' AS so_xe,
  '-' AS so_container,
  '-' AS phi_dich_vu
FROM vietful_orderline