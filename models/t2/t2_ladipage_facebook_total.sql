SELECT 
  DATE(od.inserted_at) AS date_insert,
  'Facebook' AS channel,
  od.brand,
  JSON_VALUE(od.marketer, "$.name") AS staff,
  od.total_price_after_sub_discount AS doanhThuLadi,
  CASE 
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Nhung' THEN 'LYB0000165'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Ngô Thị Ngọc Ánh' THEN 'LYB000100'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Võ Duy Khang' THEN 'LB000103'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Quốc Bình' THEN 'LB000096'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Văn Hiếu' THEN 'LB000161'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Uri Yang' THEN 'LB000090'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Minhtien Pham' THEN 'LB000146'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Digital Duy' THEN 'LB000150'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Phan Phúc' THEN 'LB000111'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Tâm Ngô' THEN 'LB000173'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Lê Trình Thanh Phước' THEN 'LB000172'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Đoàn Văn Thành Long' THEN 'LB000171'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Hoàng Văn Ninh' THEN 'LB000085'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Minh Tân' THEN 'NB0012'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Phạm Cường' THEN 'LB000166'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Ngân' THEN 'LB000179'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'LÊ VĂN TÀI' THEN 'LB000141'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Minh Vương' THEN 'LB000175'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Thanh Sang' THEN 'LB000183'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'TRẦN ĐĂNG HUY' THEN 'LB000169'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Lê Hồng Phúc' THEN 'LB000181'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Duy Đạt' THEN 'LB000167'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Chiến Thắng MKT' THEN 'LB000205'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Võ Dương Oai' THEN 'LB000081'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Ninh MKT' THEN 'LB000085'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Wên Oky' THEN 'NTB000177'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Hồ Thư' THEN 'NTB000165'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nga V' THEN 'NTB000211'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Trường Nguyễn' THEN 'NTB000182'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Hoan Đức Hoàng Công' THEN 'NTB000213'
    WHEN JSON_VALUE(od.marketer, "$.name") = 'Nguyễn Thị Thảo Nguyên' THEN 'NTB000187'
    ELSE NULL
  END AS id_staff,
  CASE 
    WHEN JSON_VALUE(od.marketer, "$.name") in ('Nguyễn Nhung','Uri Yang','Minhtien Pham','Digital Duy','Phan Phúc','Tâm Ngô') THEN 'Nguyễn Thị Tuyết Nhung'
    WHEN JSON_VALUE(od.marketer, "$.name") in ('Ngô Thị Ngọc Ánh','Quốc Bình','Văn Hiếu','Võ Duy Khang') THEN 'Ngô Thị Ngọc Ánh'
    WHEN JSON_VALUE(od.marketer, "$.name") in ('Ngô Thị Ngọc Ánh','Quốc Bình','Văn Hiếu','Võ Duy Khang') THEN 'Ngô Thị Ngọc Ánh'
    WHEN JSON_VALUE(od.marketer, "$.name") IN (
      'Lê Trình Thanh Phước',
      'Đoàn Văn Thành Long',
      'Hoàng Văn Ninh',
      'Minh Tân',
      'Phạm Cường',
      'Nguyễn Ngân',
      'LÊ VĂN TÀI',
      'Nguyễn Minh Vương',
      'Nguyễn Thanh Sang',
      'TRẦN ĐĂNG HUY',
      'Lê Hồng Phúc',
      'Nguyễn Duy Đạt',
      'Nguyễn Chiến Thắng MKT',
      'Nguyễn Võ Dương Oai',
      'Ninh MKT'
    ) THEN 'Hoàng Văn Ninh'
    WHEN JSON_VALUE(od.marketer, "$.name") in ('Hồ Thư','Nga V','Hoan Đức Hoàng Công') THEN 'Phong'
    WHEN JSON_VALUE(od.marketer, "$.name") in ('Wên Oky','Trường Nguyễn','Nguyễn Thị Thảo Nguyên') THEN 'Hửu Lộc'
    ELSE NULL
  END AS manager,
FROM {{ref('t1_pancake_pos_order_total')}} AS od
WHERE od.marketer IS NOT NULL
