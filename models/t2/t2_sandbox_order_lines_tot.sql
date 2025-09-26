WITH latest_delivery AS (
  SELECT *
  FROM {{ref("t1_sandbox_deliveries_total")}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY ngay_cap_nhat DESC, id DESC) = 1
),

deliveries as (
SELECT distinct t1.*
FROM  {{ref("t1_sandbox_deliveries_total")}} t1
JOIN latest_delivery t2
  ON t1.order_number = t2.order_number
  AND t1.ngay_cap_nhat = t2.ngay_cap_nhat
  AND t1.id = t2.id

),

orderline AS (
    SELECT 
        -- Mã đơn hàng và mã giao vận
        COALESCE(ord.order_code, NULL) AS ma_don_code,
        COALESCE(ord.order_number, NULL) AS ma_don_so,
        ord.tracking_no AS ma_giao_van,

        -- Thời gian chính
        DATETIME_ADD(ord.create_time, INTERVAL 7 HOUR) AS ngay_data_ve,
        DATETIME_ADD(ord.order_confirm_date, INTERVAL 7 HOUR) AS ngay_chot_don,
        DATETIME_ADD(ord.time_order_submit, INTERVAL 7 HOUR) AS ngay_dang_don,
        DATETIME_ADD(de.ngay_cap_nhat, INTERVAL 7 hour) as ngay_tien_ve_vi,

        -- Kho và phương thức giao hàng
        ord.warehouse_name AS kho,
        CONCAT(ord.shipping_carrier_name, ' (', ord.shipping_transport_name, ')') AS phuong_thuc_giao_hang,

        -- Tên khách hàng và địa chỉ
        ord.customer_name AS ho_ten,
        ord.customer_phone AS so_dien_thoai,
        CONCAT(
            COALESCE(ord.delivery_address, ''), ', ',
            COALESCE(ord.delivery_ward_name, ''), ', ',
            COALESCE(ord.delivery_district_name, ''), ', ',
            COALESCE(ord.delivery_province_name, '')
        ) AS dia_chi,
        ord.delivery_province_name AS tinh_giao_hang,
        
        -- CASE
        --     WHEN LOWER(ord.source_name) LIKE '%khách cũ%' THEN 'Khách cũ'
        --     WHEN ord.reason_to_create = 'FOR_TAKE_CARE' OR ord.reason_to_create = 'FROM_OLD' OR ord.reason_to_create = 'TAKECARE' OR ord.reason_to_create = 'OLD_ORDER' THEN 'Khách cũ'
        --     ELSE 'Khách mới'
        -- END AS loai_khach_hang,

        CASE 
            WHEN ord.customer_type = 0 THEN 'Khách mới'
            WHEN ord.customer_type = 1 THEN 'Khách cũ'
            ELSE 'Không xác định'
        END AS loai_khach_hang,

        CASE
            WHEN ord.reason_to_create = 'FROM_API_SHOPEE' OR ord.reason_to_create = 'FROM_API_TIKTOK' OR ord.reason_to_create = 'ECOMMERCE' THEN 'Sàn TMDT liên kết'
            WHEN ord.id_pushsale > 0 THEN 'Pushsale'
            ELSE 'Sandbox'
        END AS nguon_doanh_thu,

        -- Sản phẩm cụ thể
        dt.item_code AS sku,
        dt.item_name AS san_pham,
        dt.quantity AS so_luong,
        
        CASE
            WHEN dt.price < 1000 THEN curr.rate * dt.price
            ELSE dt.price
        END AS don_gia,

        CASE
            WHEN dt.quantity * dt.price < 1000 THEN curr.rate * dt.quantity * dt.price
            ELSE dt.quantity * dt.price
        END AS thanh_tien,

        CASE 
            WHEN discount_type = 0 THEN 0
            WHEN discount_type = 1 
                THEN ROUND(COALESCE(dt.quantity,0) * COALESCE(dt.price,0) * (COALESCE(dt.discount_value,0)/100), 0)
            WHEN discount_type =2 
                THEN COALESCE(dt.discount_value,0)
            ELSE 0  
        END AS giam_gia_san_pham,

        ROUND(SAFE_DIVIDE(dt.quantity * dt.price, NULLIF(ord.total_price, 0)) * ord.total_cod, 0) AS gia_dich_vu_vc, -- đây là số phí ship thu khách, sau khi đã trừ phần hỗ trợ vận chuyển rồi
        ROUND(SAFE_DIVIDE(dt.quantity * dt.price, NULLIF(ord.total_price, 0)) * 
            CASE WHEN ord.total_shipping_cost = 0 THEN ord.total_cod ELSE 0 END, 0) AS phi_vc_ho_tro_khach,
        ROUND(SAFE_DIVIDE(dt.quantity * dt.price, NULLIF(ord.total_price, 0)) * ord.total_deposit, 0) AS tra_truoc,
        
        -- Ghi chú & notes
        ord.delivery_note AS ghi_chu_giao_hang,
        --ord.time_take_care AS ngay_cap_nhat_care_don,
        --ord.time_take_care_work AS care_don,
        ord.note_accountant AS ghi_chu_ke_toan,
        '-' AS ngay_muon_nhan_hang,

        -- Nhân sự liên quan
        CASE 
            WHEN (ord.marketing_display_name IS NULL OR ord.marketing_display_name = '') THEN 'Admin Đơn vị'
            ELSE mar.marketing_name
        END AS marketing_name,
    
        mar.ma_nhan_vien,
        COALESCE(mar.ma_quan_ly, mar2.ma_quan_ly) AS ma_quan_ly,
        COALESCE(mar.manager, mar2.manager) AS manager,

        ord.marketing_user_name,
        ord.sale_display_name AS sale_name,
        ord.sale_user_name AS sale_user_name,

        -- Nguồn tạo đơn
        ord.source_name,

        -- Trạng thái đơn hàng
        CASE ord.order_status_id
            WHEN -1 THEN 'Hệ thống CRM đã xóa'
            WHEN 0 THEN 'Chờ chốt đơn'
            WHEN 1 THEN 'Chờ vận đơn'
            WHEN 2 THEN 'Giao ngay'
            WHEN 3 THEN 'Hoãn giao hàng'
            WHEN 4 THEN 'Hủy vận đơn'
            WHEN 5 THEN 'Hủy đăng đơn'
            WHEN 20 THEN 'Đã đăng'
            WHEN 21 THEN 'Đã lấy hàng'
            WHEN 22 THEN 'Không lấy được hàng'
            WHEN 23 THEN 'Đang lấy hàng'
            WHEN 30 THEN 'Đang giao hàng'
            WHEN 31 THEN 'Đã giao hàng'
            WHEN 32 THEN 'Đã thanh toán'
            WHEN 33 THEN 'Không giao được'
            WHEN 34 THEN 'Yêu cầu giao lại'
            WHEN 35 THEN 'Giao hàng 1 phần'
            WHEN 40 THEN 'Đang hoàn'
            WHEN 41 THEN 'Đã hoàn'
            WHEN 50 THEN 'Bồi hoàn'
            WHEN 99 THEN 'Đã xóa'
            ELSE 'Trạng thái khác'
        END AS trang_thai_don_hang,
        de.id_trang_thai_giao_hang,
        CASE cast(de.id_trang_thai_giao_hang as int64)
            WHEN -1 THEN 'Hệ thống CRM đã xóa'
            WHEN 0 THEN 'Chờ chốt đơn'
            WHEN 1 THEN 'Chờ vận đơn'
            WHEN 2 THEN 'Giao ngay'
            WHEN 3 THEN 'Hoãn giao hàng'
            WHEN 4 THEN 'Hủy vận đơn'
            WHEN 5 THEN 'Hủy đăng đơn'
            WHEN 20 THEN 'Đã đăng'
            WHEN 21 THEN 'Đã lấy hàng'
            WHEN 22 THEN 'Không lấy được hàng'
            WHEN 23 THEN 'Đang lấy hàng'
            WHEN 30 THEN 'Đang giao hàng'
            WHEN 31 THEN 'Đã giao hàng'
            WHEN 32 THEN 'Đã thanh toán'
            WHEN 33 THEN 'Không giao được'
            WHEN 34 THEN 'Yêu cầu giao lại'
            WHEN 35 THEN 'Giao hàng 1 phần'
            WHEN 40 THEN 'Đang hoàn'
            WHEN 41 THEN 'Đã hoàn'
            WHEN 50 THEN 'Bồi hoàn'
            WHEN 99 THEN 'Đã xóa'
            ELSE 'Trạng thái khác'
        END AS trang_thai_giao_hang,

        ord.operation_result_name AS ket_qua_tac_nghiep_telesale,
        bangGia.brand,
        bangGia.brand_lv1,
        'Max Eagle' AS company,
        CASE 
            WHEN (source.channel IS NULL OR source.channel = '') THEN 'Facebook'
            ELSE source.channel
        END AS channel,
        -- source.channel AS channel,

        -- Giá bán daily
        COALESCE(bangGia.gia_ban_daily, 0) AS gia_ban_daily,
        0 AS phi_van_chuyen_thuc_te,
        0 AS phi_van_chuyen_tro_gia_tu_san,
        0 AS phi_thanh_toan,
        0 AS phi_hoa_hong_shop,
        0 AS phi_hoa_hong_tiep_thi_lien_ket,
        0 AS phi_hoa_hong_quang_cao_cua_hang,
        0 AS phi_dich_vu,
        0 AS phi_xtra,

        0 AS voucher_from_seller,
        0 AS phi_co_dinh,
        0 AS seller_tro_gia,
        0 AS san_tro_gia,
        0 AS tong_phi_san,
        0 as phu_phi,
        ord.total_discount AS tong_chiet_khau_don_hang,
        ord.total_discount_product AS tong_giam_gia_san_pham_don_hang,
        ord.is_delete,
        ord.tracking_no
        
    FROM {{ref("t1_sandbox_order_detail_total")}} dt
    LEFT JOIN {{ref("t1_sandbox_order_total")}} ord ON dt.order_number = ord.order_number
    LEFT JOIN {{ref("t1_bang_gia_san_pham")}} bangGia ON TRIM(dt.item_code) = TRIM(bangGia.ma_sku)
    LEFT JOIN deliveries de on dt.order_number = de.order_number

    -- Gắn thông tin marketer cho từng đơn dựa trên marketing_user_name và ngày chốt đơn đúng trong khoảng thời gian marketer sử dụng account đó
    LEFT JOIN {{ref("t1_marketer_facebook_total")}} mar 
        ON TRIM(ord.marketing_user_name) = TRIM(mar.marketer_name) 
        AND DATE(DATETIME_ADD(ord.order_confirm_date, INTERVAL 7 HOUR)) >= DATE(mar.start_date)
        AND (mar.end_date IS NULL OR DATE(DATETIME_ADD(ord.order_confirm_date, INTERVAL 7 HOUR)) <= DATE(mar.end_date))
    -- Nếu không có marketing_user_name thì phân đơn đó về manager dựa vào ord.team
    LEFT JOIN (
        SELECT DISTINCT
            team_account,
            manager,
            ma_quan_ly,
            start_date,
            end_date,
            -- FIRST_VALUE(start_date) OVER (PARTITION BY team_account ORDER BY start_date ASC) AS start_date,
            -- FIRST_VALUE(end_date) OVER (PARTITION BY team_account ORDER BY end_date DESC) AS end_date
            FROM crypto-arcade-453509-i8.dtm.t1_marketer_facebook_total
            WHERE company = 'Max Eagle' and team_account is not null
    ) mar2 ON mar.marketer_name IS NULL AND ord.team = mar2.team_account
        AND DATE(DATETIME_ADD(ord.order_confirm_date, INTERVAL 7 HOUR)) >= DATE(mar2.start_date)
        AND (mar2.end_date IS NULL OR DATE(DATETIME_ADD(ord.order_confirm_date, INTERVAL 7 HOUR)) <= DATE(mar2.end_date))

    LEFT JOIN {{ref("t1_pushsale_source_name")}} source ON trim(ord.source_name) = trim(source.source_name) and  trim(ord.marketing_user_name) =  trim(source.marketing_user_name)
    -- LEFT JOIN deliveries de on dt.order_number = de.order_number
    LEFT JOIN {{ref("t1_pushsale_currency_rates")}} curr ON curr.currency_code = 'USD'
    WHERE cast(de.id_trang_thai_giao_hang as int64) in (31,32)
    ORDER BY de.ngay_cap_nhat ASC


),a as (
SELECT
    *,
      -- Tính chiết khấu & phí vận chuyển, trả trước dựa trên tỷ trọng sản phẩm
      CASE
        WHEN COALESCE(thanh_tien, 0) - COALESCE(giam_gia_san_pham, 0) = 0 THEN 0
        ELSE 
          ROUND(
              SAFE_DIVIDE(
                  COALESCE(thanh_tien, 0),
                  NULLIF(
                  SUM(
                      CASE 
                        WHEN COALESCE(thanh_tien, 0) - COALESCE(giam_gia_san_pham, 0) <> 0  THEN COALESCE(thanh_tien, 0) 
                        ELSE 0 
                      END) OVER (PARTITION BY ma_don_code), 
                  0
                  )
              ) * (COALESCE(tong_chiet_khau_don_hang, 0) - COALESCE(tong_giam_gia_san_pham_don_hang, 0)),
              0
          )
        END AS chiet_khau
    
FROM orderline)


select a.*,
    thanh_tien - COALESCE(chiet_khau, 0)  + (COALESCE(gia_dich_vu_vc, 0) - COALESCE(phi_vc_ho_tro_khach, 0)) - COALESCE(giam_gia_san_pham, 0)
        AS tien_khach_hang_thanh_toan,
    thanh_tien - COALESCE(chiet_khau, 0) - COALESCE(giam_gia_san_pham, 0)
        AS tien_sp_sau_tro_gia,
    COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) AS gia_ban_daily_total,
    (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - (thanh_tien - COALESCE(chiet_khau, 0) - COALESCE(giam_gia_san_pham, 0)) 
        AS tien_chiet_khau_sp,
 
    (thanh_tien - COALESCE(chiet_khau, 0) - COALESCE(giam_gia_san_pham, 0) + COALESCE(gia_dich_vu_vc, 0)- COALESCE(phi_vc_ho_tro_khach, 0)) AS doanh_thu_ke_toan, 
    CASE 
        WHEN loai_khach_hang = 'Khách mới' 
        THEN (thanh_tien - COALESCE(chiet_khau, 0) - COALESCE(giam_gia_san_pham, 0) + COALESCE(gia_dich_vu_vc, 0)- COALESCE(phi_vc_ho_tro_khach, 0))
        ELSE 0
    END AS doanh_so_moi,

    CASE 
        WHEN loai_khach_hang = 'Khách cũ' 
        THEN (thanh_tien - COALESCE(chiet_khau, 0) - COALESCE(giam_gia_san_pham, 0) + COALESCE(gia_dich_vu_vc, 0)- COALESCE(phi_vc_ho_tro_khach, 0))
        ELSE 0
    END AS doanh_so_cu
from a where is_delete is not true and nguon_doanh_thu <> 'Sàn TMDT liên kết'
