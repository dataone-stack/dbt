WITH  deliveries as (
    SELECT t1.*
FROM {{ref("t1_pushsale_deliveries_total")}} t1
WHERE t1.update_date = (
    SELECT MAX(t2.update_date)
    FROM `pushsale_maxeagle_dwh.maxeagle_pushsale_deliveries` t2
    WHERE t2.order_number = t1.order_number
)
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
        DATETIME_ADD(ord.update_time, INTERVAL 7 HOUR) AS ngay_cap_nhat,

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
        CASE 
            WHEN ord.customer_type = 0 THEN 'Khách hàng mới'
            WHEN ord.customer_type = 1 THEN 'Khách hàng cũ'
            ELSE 'Không xác định'
        END AS loai_khach_hang,

        -- Sản phẩm cụ thể
        dt.item_code AS sku,
        dt.item_name AS san_pham,
        dt.quantity AS so_luong,
        dt.price AS don_gia,
        SAFE_MULTIPLY(dt.quantity, dt.price) AS thanh_tien,

        -- Tính chiết khấu & phí vận chuyển, trả trước dựa trên tỷ trọng sản phẩm
        ROUND(SAFE_DIVIDE(dt.quantity * dt.price, NULLIF(ord.total_price, 0)) * ord.total_discount, 0) AS chiet_khau,
        dt.discount AS giam_gia_san_pham,
        ROUND(SAFE_DIVIDE(dt.quantity * dt.price, NULLIF(ord.total_price, 0)) * ord.total_cod, 0) AS gia_dich_vu_vc,
        ROUND(SAFE_DIVIDE(dt.quantity * dt.price, NULLIF(ord.total_price, 0)) * 
            CASE WHEN ord.total_shipping_cost = 0 THEN ord.total_cod ELSE 0 END, 0) AS phi_vc_ho_tro_khach,
        ROUND(SAFE_DIVIDE(dt.quantity * dt.price, NULLIF(ord.total_price, 0)) * ord.total_deposit, 0) AS tra_truoc,

        -- Ghi chú & notes
        ord.delivery_note AS ghi_chu_giao_hang,
        ord.time_take_care AS ngay_cap_nhat_care_don,
        ord.time_take_care_work AS care_don,
        ord.note_accountant AS ghi_chu_ke_toan,
        '-' AS ngay_muon_nhan_hang,

        -- Nhân sự liên quan
        ord.marketing_display_name AS marketing_name,
        ord.marketing_user_name AS marketing_user_name,
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
        de.delivery_status,
        CASE de.delivery_status
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
        'Max Eagle' AS company,

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
        0 AS tong_phi_san
    FROM {{ ref('t1_pushsale_order_line_total') }} dt
    LEFT JOIN {{ ref('t1_pushsale_order_total') }} ord ON dt.order_number = ord.order_number
    LEFT JOIN {{ ref('t1_bang_gia_san_pham') }} bangGia ON dt.item_code = bangGia.ma_sku
    LEFT JOIN deliveries de on dt.order_number = de.order_number
    ORDER BY ngay_chot_don ASC
)
SELECT
    *,
    thanh_tien - COALESCE(chiet_khau, 0) - COALESCE(giam_gia_san_pham, 0) + (COALESCE(gia_dich_vu_vc, 0) - COALESCE(phi_vc_ho_tro_khach, 0)) AS tien_khach_hang_thanh_toan,
    thanh_tien - COALESCE(giam_gia_san_pham, 0) AS tien_sp_sau_tro_gia,
    COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0) AS gia_ban_daily_total,
    (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - (thanh_tien - COALESCE(chiet_khau, 0) - COALESCE(giam_gia_san_pham, 0)) AS tien_chiet_khau_sp,
    (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - (
        (COALESCE(gia_ban_daily, 0) * COALESCE(so_luong, 0)) - 
        (thanh_tien - COALESCE(chiet_khau, 0) + (COALESCE(gia_dich_vu_vc, 0) - COALESCE(phi_vc_ho_tro_khach, 0)))
    ) AS doanh_thu_ke_toan
FROM orderline