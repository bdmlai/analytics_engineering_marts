{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["truncate fds_cpg.rpt_cpg_weekly_talent_top25_shop_sales"],
		"materialized": 'incremental','tags': "Phase 5B"
        })
}}
select distinct a.*, b.revenue as last_week_revenue, b.units as last_week_units, b.rank_last_week, c.top_product,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(select s.etl_insert_rec_dttm as created_timestamp, s.dim_shop_site_id, cast(cast(s.date_key as varchar(10)) as date),
trunc(date_trunc('week',cast(cast(s.date_key as varchar(10)) as date))) as week_start,
s.src_order_type, c.src_channel_name, ss.site_name,
case 
when i.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when i.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when i.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when i.src_talent_description like '%ELIAS%' then 'ELIAS' 
when i.src_talent_description like '%BIG E%' then 'BIG E' 
when i.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else i.src_talent_description 
end as talent_description,
i.src_style_id, i.src_style_description, s.dim_item_id, sum(s.src_units_ordered) as units,
sum(s.demand_sales_$) as revenue 
from {{source('fds_cpg','aggr_cpg_daily_sales')}} s, {{source('fds_cpg','dim_cpg_item')}} i, 
{{source('fds_cpg','dim_cpg_order_method')}} c, {{source('fds_cpg','dim_cpg_shop_site')}} ss
where s.dim_item_id = i.dim_item_id and s.dim_order_method_id = c.dim_order_method_id 
and s.dim_shop_site_id = ss.dim_shop_site_id and week_start = trunc(date_trunc('week',(current_date - 7)))
group by s.etl_insert_rec_dttm, s.dim_shop_site_id, cast(cast(s.date_key as varchar(10)) as date), s.src_order_type, 
c.src_channel_name, ss.site_name, i.src_style_id, i.src_talent_description, i.src_style_description, s.dim_item_id) a
left join
(select *, row_number() over (order by revenue desc) as rank_last_week 
from
(select trunc(date_trunc('week',cast(cast(s.date_key as varchar(10)) as date))) as week_start,
case 
when i.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when i.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when i.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when i.src_talent_description like '%ELIAS%' then 'ELIAS' 
when i.src_talent_description like '%BIG E%' then 'BIG E' 
when i.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else i.src_talent_description 
end as talent_description,
sum(s.src_units_ordered) as units, sum(s.demand_sales_$) as revenue 
from {{source('fds_cpg','aggr_cpg_daily_sales')}} s, {{source('fds_cpg','dim_cpg_item')}} i
where week_start = trunc(date_trunc('week',(current_date - 14))) and s.dim_item_id = i.dim_item_id 
and s.src_units_ordered > 0 and s.dim_shop_site_id in (1, 2, 3, 4, 5)
and src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
group by 1,2)) b
on a.talent_description = b.talent_description
left join
(select talent_description, src_style_description as top_product
from
(select 
case 
when i.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when i.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when i.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when i.src_talent_description like '%ELIAS%' then 'ELIAS' 
when i.src_talent_description like '%BIG E%' then 'BIG E'
when i.src_talent_description like '%APOLLO%' then 'APOLLO CREWS' 
else i.src_talent_description 
end as talent_description, 
i.src_style_description, sum(s.demand_sales_$) as revenue, row_number() over (partition by talent_description order by revenue desc) as rn
from {{source('fds_cpg','aggr_cpg_daily_sales')}} s, {{source('fds_cpg','dim_cpg_item')}} i
where s.dim_item_id = i.dim_item_id and
trunc(date_trunc('week',cast(cast(s.date_key as varchar(10)) as date))) = trunc(date_trunc('week',(current_date - 7)))
group by talent_description, i.src_style_id, i.src_style_description
order by talent_description)
where rn = 1) c
on a.talent_description = c.talent_description