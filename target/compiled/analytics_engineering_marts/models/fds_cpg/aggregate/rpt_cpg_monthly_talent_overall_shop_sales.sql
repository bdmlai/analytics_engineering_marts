
with __dbt__CTE__intm_cpg_monthly_talent_shop_sales as (

select z.*, y.cust_cnt as customers_cnt
from 
(select b.gender_description_product, b.style_category, b.product_category, b.talent_description, 
active_inactive_flag, gender as gender_talent, designation as brand, website, a.order_month,
case 
when datediff('day', style_launched_month, a.order_Month ) <= 90 
then 'Yes' 
else 'No' end style_launched_90_days,
style_launched_month, round(avg(datediff('day',style_launched_month, a.order_Month))/30) avg_age_style_month,
sum(revenue) as revenue, sum(margin) as margin, sum(units_ordered) as units_ordered,
sum(isnull(revenue,0))/nullif(sum(units_ordered),0) as avg_unit_price
from 
(select date_trunc('month', cast(cast(g.date_key as varchar(10)) as date)) order_month, dim_item_id, 
dim_shop_site_id, sum(g.src_units_ordered) as units_ordered, avg(g.src_unit_cost) as avg_unit_cost,
sum(isnull(demand_sales_$, 0)) as revenue, sum(isnull(demand_selling_margin_$, 0)) as margin,
(sum(isnull(demand_sales_$,0))/ nullif(sum(g.src_units_ordered), 0)) as avg_unit_price
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" g 
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on g.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and date_key > 20151231 and order_month <  date_trunc('month', current_date) 
group by 1,2,3) a 
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
from "entdwdb"."fds_cpg"."dim_cpg_item" a 
where a.src_talent_description not in 
('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA','EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK',
'ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')) b on a.dim_item_id = b.dim_item_id
left join 
(select dim_shop_site_id, site_name website 
from "entdwdb"."fds_cpg"."dim_cpg_shop_site") s on a.dim_shop_site_id = s.dim_shop_site_id
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
min(gender) gender, designation
from 
(select a.src_talent_description, c.character_lineage_name, c.character_lineages_wweid, c.gender, characters_wweid
from "entdwdb"."fds_cpg"."dim_cpg_item" a
left join  
(select * 
from "entdwdb"."fds_mdm"."alternateid" 
where alternate_id_type_name = 'Merch Sales') b on a.src_talent_id = b.alternate_id_name
left join "entdwdb"."fds_mdm"."character" c on b.entity_id = c.character_lineage_id
group by 1,2,3,4,5) h
left join 
(select a.* from 
(select character_lineage_wweid, designation, start_date
from "entdwdb"."fds_emm"."brand") a inner join 
(select character_lineage_wweid, max(start_date) as start_date 
from "entdwdb"."fds_emm"."brand" group by 1) b
on a.character_lineage_wweid = b.character_lineage_wweid and a.start_date = b.start_date) d
on h.character_lineages_wweid = d.character_lineage_wweid 
group by 1,3) t on b.talent_description = t.talent_description  
left join
(select
case 
when n.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when n.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when n.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when n.src_talent_description like '%ELIAS%' then 'ELIAS' 
when n.src_talent_description like '%BIG E%' then 'BIG E'
when n.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else n.src_talent_description  
end talent_description, 
src_major_category_description, src_style_description, 
min(date_trunc('month',cast(cast(m.date_key as varchar(10)) as date))) as style_launched_month 
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" m 
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on m.dim_order_method_id = j.dim_order_method_id
left join "entdwdb"."fds_cpg"."dim_cpg_item" n on m.dim_item_id = n.dim_item_id
where isnull(j.src_channel_id, '0') <> 'R' and  m.date_key > 20151231
group by 1,2,3) o on b.talent_description = o.talent_description and b.product_category = o.src_major_category_description
and b.style_category = o.src_style_description
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
date_trunc('month', cast(cast(c.order_date_id as varchar(10)) as date)) order_month,
d.site_name website, count(distinct c.dim_customer_email_address_id) cust_cnt
from "entdwdb"."fds_cpg"."dim_cpg_item" b
left join "entdwdb"."fds_cpg"."fact_cpg_sales_detail" c on b.dim_item_id = c.dim_item_id
and b.src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK', 'ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
left join "entdwdb"."fds_cpg"."dim_cpg_shop_site" d on c.dim_shop_site_id = d.dim_shop_site_id
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on c.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R'  and c.order_date_id > 20151231 and c.src_units_ordered > 0 
group by 1,2,3,4,5) y on z.talent_description = y.talent_description and z.product_category = y.product_category
and z.style_category = y.style_category and z.order_month = y.order_month and z.website = y.website 
where z.talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
)select a.*, py_styles_selling, py_revenue, py_margin, 
py_units_ordered, py_avg_unit_price, py_cust_count, py_new_styles,
pm_rev_rank, pm_units_rank, pm_style_sold_rank, pm_avg_unit_p_rank,
AUP_percentile, pm_sale_data.rev as pm_revenue, pm_margin, pm_sale_data.units as pm_units_ordered,
pm_sale_data.avg_unit_p as pm_avg_unit_price, pm_cust_count, pm_styles_selling, pm_overall_revenue,
pm_overall_margin, pm_overall_units_ordered, pm_overall_avg_unit_price, pm_overall_styles_selling,
overall_revenue, overall_margin, overall_units_ordered, overall_avg_unit_price, overall_styles_selling,
rev_rank, units_rank, style_sold_rank, avg_unit_p_rank, pm_new_styles,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from __dbt__CTE__intm_cpg_monthly_talent_shop_sales a
left join 
(select dateadd('year', 1, order_month)  as order_month, talent_description ,
count(distinct style_category) as py_styles_selling, sum(revenue) as py_revenue, 
sum(margin) as py_margin, sum(units_ordered) as py_units_ordered,
(sum(revenue)/nullif(sum(units_ordered),0)) as py_avg_unit_price, sum(customers_cnt) as py_cust_count
from __dbt__CTE__intm_cpg_monthly_talent_shop_sales
group by 1,2) py_sale_data
on a.talent_description = py_sale_data.talent_description
and a.order_month = py_sale_data.order_month
left join
(select dateadd('year', 1, order_month) as order_month, talent_description, count(distinct style_category) as py_new_styles
from __dbt__CTE__intm_cpg_monthly_talent_shop_sales where style_launched_90_days = 'Yes'
group by 1,2) py_new_style_data
on a.talent_description = py_new_style_data.talent_description
and a.order_month = py_new_style_data.order_month
left join 
(select date_add('month', 1, order_month) as order_month, talent_description, count(distinct style_category) as pm_new_styles
from  __dbt__CTE__intm_cpg_monthly_talent_shop_sales where style_launched_90_days = 'Yes'
group by 1,2) pm_new_style_data
on a.order_month = pm_new_style_data.order_month
and a.talent_description = pm_new_style_data.talent_description
left join
(select order_month, talent_description, rev, units, style_sold, avg_unit_p,
	row_number() over (partition by order_month order by rev desc) as rev_rank,
	row_number() over (partition by order_month order by units desc) as units_rank,
	row_number() over (partition by order_month order by style_sold desc) as style_sold_rank,
	row_number() over (partition by order_month order by avg_unit_p desc) as avg_unit_p_rank,
	round((percent_rank() over (partition by order_month order by avg_unit_p asc))*100,2) as AUP_percentile
from
(select date_trunc('month', cast(cast(date_key as varchar(10)) as date)) order_month, talent_description,
sum(isnull(demand_sales_$,0))  as rev, sum(src_units_ordered) as units, count(distinct src_style_description) as style_sold,
(sum(isnull(demand_sales_$,0))/nullif(sum(src_units_ordered),0)) as avg_unit_p from   
"entdwdb"."fds_cpg"."aggr_cpg_daily_sales" m inner join
(select v.dim_item_id,
case 
when v.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when v.src_talent_description ='RANDOM' then 'RONDA ROUSEY' 
when v.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when v.src_talent_description like '%ELIAS%' then 'ELIAS' 
when v.src_talent_description like '%BIG E%' then 'BIG E'
when v.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else v.src_talent_description 
end talent_description, v.src_style_description 
from "entdwdb"."fds_cpg"."dim_cpg_item" v
where v.src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')) n 
on m.dim_item_id = n.dim_item_id
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and date_key > 20151231  group by 1,2)) cm_rank_data
on a.talent_description = cm_rank_data.talent_description
and a.order_month = cm_rank_data.order_month
left join 
(select date_trunc('month', cast(cast(date_key as varchar(10)) as date)) order_month,
sum(isnull(demand_sales_$,0)) as pm_overall_revenue, sum(isnull(demand_selling_margin_$,0)) as pm_overall_margin,
sum(src_units_ordered)as pm_overall_units_ordered, (sum(isnull(demand_sales_$,0))/nullif(sum(src_units_ordered),0)) as pm_overall_avg_unit_price, count(distinct src_style_description) as pm_overall_styles_selling
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" m 
left join "entdwdb"."fds_cpg"."dim_cpg_item" n on m.dim_item_id = n.dim_item_id
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and  m.date_key > 20151231
group by 1) pm_overall_data
on a.order_month = date_add('month', 1, pm_overall_data.order_month)
left join 
(select date_trunc('month', cast(cast(date_key as varchar(10)) as date)) order_month,
sum(isnull(demand_sales_$,0)) as overall_revenue, sum(isnull(demand_selling_margin_$,0)) as overall_margin,
sum(src_units_ordered)as overall_units_ordered, (sum(isnull(demand_sales_$,0))/nullif(sum(src_units_ordered),0)) as overall_avg_unit_price,
count(distinct src_style_description) as overall_styles_selling
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" m 
left join "entdwdb"."fds_cpg"."dim_cpg_item" n on m.dim_item_id = n.dim_item_id
left join "entdwdb"."fds_cpg"."dim_cpg_order_method" j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and  m.date_key > 20151231
 group by 1) cm_overall_data
on a.order_month = cm_overall_data.order_month
left join
(select date_add('month', 1, order_month) order_month, talent_description, 
rev, units, style_sold, avg_unit_p, pm_margin, pm_cust_count,pm_styles_selling,
row_number() over (partition by order_month order by rev desc) as pm_rev_rank,
row_number() over (partition by order_month order by units desc) as pm_units_rank,
row_number() over (partition by order_month order by style_sold desc) as pm_style_sold_rank,
row_number() over (partition by order_month order by avg_unit_p desc) as pm_avg_unit_p_rank
from
(select order_month, talent_description, sum(revenue) as rev, sum(units_ordered) as units, count(distinct style_category) as style_sold,
(sum(revenue)/nullif(sum(units_ordered),0)) as avg_unit_p, sum(margin) as pm_margin, sum(customers_cnt) as pm_cust_count,
count(distinct style_category) as pm_styles_selling
from __dbt__CTE__intm_cpg_monthly_talent_shop_sales 
group by 1,2)) pm_sale_data
on a.order_month = pm_sale_data.order_month
and a.talent_description = pm_sale_data.talent_description