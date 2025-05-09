SELECT
    a.date_start,
    a.brand,
    a.channel,
    (a.doanhThuAds + a.doanhThuLadi + a.doanhThuGMVTiktok) AS totalAds,
    sum(p.total_price_after_sub_discount)
FROM {{ref("t3_ads_total_with_tkqc")}} a
full outer join {{(ref("t1_pancake_pos_order_total"))}} p
    ON a.date_start = date(p.inserted_at)
    AND a.brand = p.brand
    AND a.channel = p.order_sources_name
group by a.date_start,
    a.brand,
    a.channel,
    totalAds