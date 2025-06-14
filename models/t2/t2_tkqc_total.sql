select
    tkqc.idtkqc,
    tkqc.nametkqc,
    tkqc.ben_thue,
    tkqc.phi_thue,
    tkqc.dau_the,
    tkqc.ma_nhan_vien,
    tkqc.staff,
    tkqc.ma_quan_ly,
    tkqc.manager,
    tkqc.channel,
    tkqc.status,
    tkqc.start_date,
    tkqc.end_date,
    tkqc.company,
    case
        when tkqc.sku != ""
        then sp.brand
        else tkqc.brand
    end as brand
from {{(ref("t1_tkqc"))}} tkqc
left join {{ref("t1_bang_gia_san_pham")}} sp
on tkqc.sku = sp.ma_sku