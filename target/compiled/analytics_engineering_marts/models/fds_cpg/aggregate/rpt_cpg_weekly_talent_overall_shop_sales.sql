
with __dbt__CTE__intm_cpg_weekly_talent_shop_sales as (

select z.*, y.cust_cnt as customers_cnt 
from 
(select a.gender_description_product, a.style_category, a.product_category, a.talent_description,
a.active_inactive_flag, gender as gender_talent, designation as brand, s.site_name as website,
date_trunc('week', cast(cast(g.date_key as varchar(10)) as date)) as order_week,
case 
when datediff('day',style_launched_month, order_week ) <= 90 then 'Yes' 
else 'No' end style_launched_90_days,
style_launched_month, round(avg(datediff('day',style_launched_month, order_week) )/30) avg_age_style_month,
sum(nvl(demand_sales_$,0)) as revenue, sum(nvl(demand_selling_margin_$,0)) as margin,
sum(g.src_units_ordered) as units_ordered, sum(nvl(demand_sales_$,0))/nullif(sum(g.src_units_ordered),0) as avg_unit_price,
avg(g.src_unit_cost) as avg_unit_cost 
from 
"entdwdb"."fds_cpg"."aggr_cpg_daily_sales" g
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" k on g.dim_order_method_id = k.dim_order_method_id
left join  
(select a.src_gender_description as gender_description_product, dim_item_id,
a.src_style_description as style_category, a.src_major_category_description as product_category,
case 
when a.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when a.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when a.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when a.src_talent_description like '%ELIAS%' then 'ELIAS' 
when a.src_talent_description like '%BIG E%' then 'BIG E'
when a.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else a.src_talent_description 
end talent_description,
src_talent_id,
case
when upper(a.src_active_inactive_flag) = 'YES' then 'Y'
when upper(a.src_active_inactive_flag) = 'NO' then 'N'
else a.src_active_inactive_flag
end active_inactive_flag
from 
"entdwdb"."fds_cpg"."dim_cpg_item" a 
where a.src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA','EVOLUTION WOMEN''S PPV',
'TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')) a on g.dim_item_id = a.dim_item_id
left join "entdwdb"."fds_cpg"."dim_cpg_shop_site" s on g.dim_shop_site_id = s.dim_shop_site_id 
left join 
(select
case 
when n.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when n.src_talent_description='RANDOM' then 'RONDA ROUSEY' 
when n.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when n.src_talent_description like '%ELIAS%' then 'ELIAS' 
when n.src_talent_description like '%BIG E%' then 'BIG E'
when n.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else n.src_talent_description 
end talent_description,
src_major_category_description, src_style_description,
min(date_trunc('week',cast(cast(m.date_key as varchar(10)) as date))) style_launched_month
from  
"entdwdb"."fds_cpg"."aggr_cpg_daily_sales" m 
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on m.dim_order_method_id = j.dim_order_method_id
left join "entdwdb"."fds_cpg"."dim_cpg_item" n on m.dim_item_id = n.dim_item_id
where isnull(j.src_channel_id, '0') <> 'R' and  m.date_key > 20151231
group by 1,2,3) o on a.talent_description = o.talent_description and a.product_category = o.src_major_category_description
and a.style_category = o.src_style_description  
left join  
(select   
case 
when h.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when h.src_talent_description='RANDOM' then 'RONDA ROUSEY' 
when h.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when h.src_talent_description like '%ELIAS%' then 'ELIAS' 
when h.src_talent_description like '%BIG E%' then 'BIG E'
when h.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else h.src_talent_description 
end talent_description,
min(gender) as gender, designation
from 
(select a.src_talent_description, c.character_lineage_name, c.character_lineages_wweid, c.gender, characters_wweid
from "entdwdb"."fds_cpg"."dim_cpg_item" a
left join  
(select alternate_id_name, entity_id 
from "entdwdb"."fds_mdm"."alternateid" 
where alternate_id_type_name = 'Merch Sales') b on a.src_talent_id = b.alternate_id_name
left join "entdwdb"."fds_mdm"."character" c on b.entity_id = c.character_lineage_id
group by 1,2,3,4,5) h
left join 
(select a.* from 
(select character_lineage_wweid, designation, start_date
from "entdwdb"."fds_emm"."brand") a 
inner join 
(select character_lineage_wweid, max(start_date) as start_date from "entdwdb"."fds_emm"."brand" group by 1) b
on a.character_lineage_wweid = b.character_lineage_wweid and a.start_date = b.start_date) d
on h.character_lineages_wweid = d.character_lineage_wweid
group by 1,3) t on a.talent_description = t.talent_description   
where  isnull(k.src_channel_id, '0') <> 'R'  and order_week >= '01-JAN-16' and order_week <  date_trunc('week', current_date)   
group by 1,2,3,4,5,6,7,8,9,10,11) z
left join
(select b.src_style_description as style_category, b.src_major_category_description as product_category,
case 
when b.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when b.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when b.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when b.src_talent_description like '%ELIAS%' then 'ELIAS' 
when b.src_talent_description like '%BIG E%' then 'BIG E'
when b.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else b.src_talent_description 
end talent_description,
date_trunc('week', cast(cast(c.order_date_id as varchar(10)) as date)) order_week, d.site_name website,
count(distinct c.dim_customer_email_address_id) cust_cnt
from "entdwdb"."fds_cpg"."dim_cpg_item" b  
left join "entdwdb"."fds_cpg"."fact_cpg_sales_detail" c on b.dim_item_id = c.dim_item_id
and b.src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK', 'ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
left join "entdwdb"."fds_cpg"."dim_cpg_shop_site" d on c.dim_shop_site_id = d.dim_shop_site_id
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on c.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and c.order_date_id > 20151231 and c.src_units_ordered > 0
group by 1,2,3,4,5) y on z.talent_description = y.talent_description and z.product_category = y.product_category
and z.style_category = y.style_category and z.order_week = y.order_week and z.website = y.website 
where z.talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
)select a.gender_description_product, a.style_category, a.product_category, a.talent_description, a.active_inactive_flag,
a.gender_talent, website, style_launched_90_days, style_launched_month, avg_age_style_month, a.brand, a.order_week,
pw_rev_rank, pw_units_rank, pw_style_sold_rank, pw_avg_unit_p_rank, rev_rank, units_rank, style_sold_rank, avg_unit_p_rank,
overall_revenue, overall_margin, overall_units_ordered, overall_avg_unit_price, overall_styles_selling,
pw_overall_revenue, pw_overall_margin, pw_overall_units_ordered, pw_overall_avg_unit_price, pw_overall_styles_selling,
sum(revenue) as revenue, sum(margin) as margin, sum(units_ordered) as units_ordered, sum(revenue)/nullif(sum(units_ordered),0) as avg_unit_price,sum(customers_cnt) customers_cnt, sum(py_styles_selling) as py_styles_selling, sum(py_revenue) as py_revenue, sum(py_margin) as py_margin,
sum(py_units_ordered) as py_units_ordered, sum(py_avg_unit_price) as py_avg_unit_price, sum(py_cust_count) as py_cust_count, 
sum(py_new_styles) as py_new_styles, sum(AUP_percentile) as AUP_percentile, sum(pw_sale_data.rev) as pw_revenue,
sum(pw_sale_data.pw_margin) as pw_margin, sum(pw_sale_data.units) as pw_units_ordered, avg(pw_sale_data.avg_unit_p) as pw_avg_unit_price,
sum(pw_cust_count) as pw_cust_count, sum(pw_styles_selling) as pw_styles_selling, sum(pw_new_styles) as pw_new_styles,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
__dbt__CTE__intm_cpg_weekly_talent_shop_sales a
left join 
(select order_week, talent_description, count(distinct style_category) as py_styles_selling,
sum(revenue) as py_revenue, sum(margin) as py_margin, sum(units_ordered) as py_units_ordered, 
(sum(revenue)/nullif(sum(units_ordered),0)) as py_avg_unit_price, sum(customers_cnt) as py_cust_count
from __dbt__CTE__intm_cpg_weekly_talent_shop_sales
group by 1,2) py_sale_data
on a.talent_description = py_sale_data.talent_description
and a.order_week = date_trunc('week', dateadd('week', 52, py_sale_data.order_week))
left join
(select order_week, talent_description, count(distinct style_category) as py_new_styles
from __dbt__CTE__intm_cpg_weekly_talent_shop_sales 
where style_launched_90_days = 'Yes'
group by 1,2) py_new_style_data
on a.talent_description = py_new_style_data.talent_description
and a.order_week = date_trunc('week', dateadd('week', 52, py_new_style_data.order_week))
left join
(select order_week, talent_description, rev, units, style_sold, avg_unit_p, pw_margin, pw_cust_count, pw_styles_selling,
row_number() over (partition by order_week order by rev desc) as pw_rev_rank,
row_number() over (partition by order_week order by units desc) as pw_units_rank,
row_number() over (partition by order_week order by style_sold desc) as pw_style_sold_rank,
row_number() over (partition by order_week order by avg_unit_p desc) as pw_avg_unit_p_rank
from
(select order_week, talent_description, sum(revenue) as rev, sum(units_ordered) as units, count(distinct style_category) as style_sold,
sum(margin) as pw_margin, sum(customers_cnt) as pw_cust_count, count(distinct style_category) as pw_styles_selling,
(sum(revenue)/nullif(sum(units_ordered),0)) as avg_unit_p
from __dbt__CTE__intm_cpg_weekly_talent_shop_sales 
group by 1,2)) pw_sale_data
on a.order_week = dateadd('day', 7, pw_sale_data.order_week)
and a.talent_description = pw_sale_data.talent_description
left join 
(select date_trunc('week', cast(cast(date_key as varchar(10)) as date)) as order_week, 
sum(isnull(demand_sales_$,0)) as pw_overall_revenue, sum(isnull(demand_selling_margin_$,0)) as pw_overall_margin,
sum(src_units_ordered) as pw_overall_units_ordered, sum(nvl(demand_sales_$,0))/nullif(sum(src_units_ordered),0) as pw_overall_avg_unit_price,
count(distinct n.src_style_description) as pw_overall_styles_selling
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" m 
left join "entdwdb"."fds_cpg"."dim_cpg_item" n on m.dim_item_id = n.dim_item_id
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and m.date_key > 20151231
group by 1) pw_overall_data on a.order_week = dateadd('day', 7, pw_overall_data.order_week)
left join 
(select date_trunc('week', cast(cast(date_key as varchar(10)) as date)) order_week,
sum(isnull(demand_sales_$,0)) as overall_revenue, sum(isnull(demand_selling_margin_$,0)) as overall_margin,
sum(src_units_ordered) as overall_units_ordered, sum(nvl(demand_sales_$,0))/nullif(sum(src_units_ordered),0) as overall_avg_unit_price,
count(distinct n.src_style_description) as overall_styles_selling
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" m 
left join "entdwdb"."fds_cpg"."dim_cpg_item" n on m.dim_item_id = n.dim_item_id
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and m.date_key > 20151231
group by 1) cw_overall_data on a.order_week = cw_overall_data.order_week
left join 
(select order_week, talent_description, count(distinct style_category) as pw_new_styles
from __dbt__CTE__intm_cpg_weekly_talent_shop_sales where style_launched_90_days = 'Yes'
group by 1, 2) pw_new_style_data
on a.order_week = dateadd('day', 7, pw_new_style_data.order_week) and 
a.talent_description = pw_new_style_data.talent_description
left join
(select order_week, talent_description,rev,units,style_sold,avg_unit_p,
row_number() over (partition by order_week order by rev desc) as rev_rank,
row_number() over (partition by order_week order by units desc) as units_rank,
row_number() over (partition by order_week order by style_sold desc) as style_sold_rank,
row_number() over (partition by order_week order by avg_unit_p desc) as avg_unit_p_rank,
round((percent_rank() over (partition by order_week order by avg_unit_p asc))*100,2) as AUP_percentile
from
(select order_week, talent_description,sum(revenue) as rev,sum(units_ordered) as units,  
count(distinct style_category) as style_sold,(sum(revenue)/nullif(sum(units_ordered),0)) as avg_unit_p 
from __dbt__CTE__intm_cpg_weekly_talent_shop_sales
group by 1,2)) cw_rank_data
on a.talent_description= cw_rank_data.talent_description and 
a.order_week= cw_rank_data.order_week
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30