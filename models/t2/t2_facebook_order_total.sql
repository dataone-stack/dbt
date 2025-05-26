SELECT 
    ord.id,
    ord.brand,
    ord.inserted_at,
    ord.updated_at,
    ord.status_name,
    ord.returned_reason_name,
    ord.page_id,
    ord.marketer_name,
    ord.ten_nguoi_mua,
    sum(ord.quantity) as so_luong_ban ,
    sum(ord.gia_san_pham) as gia_san_pham,
    sum(ord.tong_so_tien) as tong_so_tien,
    sum(ord.khuyen_mai_dong_gia) as khuyen_mai_dong_gia,
    sum(ord.giam_gia_don_hang) as giam_gia_don_hang,
    sum(ord.phi_van_chuyen) as phi_van_chuyen,
    sum(ord.tra_truoc) as tra_truoc,
    sum(ord.tong_tien_can_thanh_toan) as tong_tien_can_thanh_toan,
    sum(ord.cod) as cod,
    sum(pos.total_price_after_sub_discount) as test
FROM {{ref("t2_facebook_order_lines_total")}} as ord
left join {{ref("t1_pancake_pos_order_total")}} as pos
on ord.id = pos.id and ord.brand = pos.brand
group BY
    ord.id,
    ord.brand,
    ord.inserted_at,
    ord.updated_at,
    ord.status_name,
    ord.returned_reason_name,
    ord.page_id,
    ord.marketer_name,
    ord.ten_nguoi_mua