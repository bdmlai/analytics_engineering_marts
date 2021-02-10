{{
  config({
		"materialized": 'ephemeral'
  })
}}
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
from {{source('fds_cpg','aggr_cpg_daily_sales')}} g 
left join {{source('fds_cpg','dim_cpg_order_method')}} j on g.dim_order_method_id = j.dim_order_method_id
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
from {{source('fds_cpg','dim_cpg_item')}} a 
where a.src_talent_description not in 
('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA','EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK',
'ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')) b on a.dim_item_id = b.dim_item_id
left join 
(select dim_shop_site_id, site_name website 
from {{source('fds_cpg','dim_cpg_shop_site')}}) s on a.dim_shop_site_id = s.dim_shop_site_id
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
from {{source('fds_cpg','dim_cpg_item')}} a
left join  
(select * 
from {{source('fds_mdm','alternateid')}} 
where alternate_id_type_name = 'Merch Sales') b on a.src_talent_id = b.alternate_id_name
left join {{source('fds_mdm','character')}} c on b.entity_id = c.character_lineage_id
group by 1,2,3,4,5) h
left join 
(select a.* from 
(select character_lineage_wweid, designation, start_date
from {{source('fds_emm','brand')}}) a inner join 
(select character_lineage_wweid, max(start_date) as start_date 
from {{source('fds_emm','brand')}} group by 1) b
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
from {{source('fds_cpg','aggr_cpg_daily_sales')}} m 
left join {{source('fds_cpg','dim_cpg_order_method')}} j on m.dim_order_method_id = j.dim_order_method_id
left join {{source('fds_cpg','dim_cpg_item')}} n on m.dim_item_id = n.dim_item_id
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
from {{source('fds_cpg','dim_cpg_item')}} b
left join {{source('fds_cpg','fact_cpg_sales_detail')}} c on b.dim_item_id = c.dim_item_id
and b.src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK', 'ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
left join {{source('fds_cpg','dim_cpg_shop_site')}} d on c.dim_shop_site_id = d.dim_shop_site_id
left join {{source('fds_cpg','dim_cpg_order_method')}} j on c.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R'  and c.order_date_id > 20151231 and c.src_units_ordered > 0 
group by 1,2,3,4,5) y on z.talent_description = y.talent_description and z.product_category = y.product_category
and z.style_category = y.style_category and z.order_month = y.order_month and z.website = y.website 
where z.talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')