{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["truncate fds_cpg.rpt_cpg_monthly_talent_top25_venue_sales"],
		"materialized": 'incremental','tags': "Phase 5B"
        })
}}
select a.*, b.revenue as last_month_revenue, b.units as last_month_units, b.rank_last_month,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(select s.etl_insert_rec_dttm as created_timestamp, cast(cast(s.date_id as varchar(10)) as date),
trunc(date_trunc('month',cast(cast(s.date_id as varchar(10)) as date))) as month,
case 
when i.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when i.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when i.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when i.src_talent_description like '%ELIAS%' then 'ELIAS' 
when i.src_talent_description like '%BIG E%' then 'BIG E' 
when i.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else i.src_talent_description 
end as talent_description,
i.src_style_id, i.src_style_description, s.dim_item_id, sum(s.net_units_sold) as units, sum(s.total_revenue) as revenue 
from {{source('fds_cpg','aggr_cpg_daily_venue_sales')}} s, {{source('fds_cpg','dim_cpg_item')}} i
where s.dim_item_id = i.dim_item_id and 
month = trunc(date_trunc('month',add_months(current_date,-1))) and s.active_flag = 'Y'
group by s.etl_insert_rec_dttm, cast(cast(s.date_id as varchar(10)) as date), i.src_talent_description, 
i.src_style_description, i.src_style_id, s.dim_item_id
order by date_id desc) a
left join
(select *, row_number() over (order by revenue desc) as rank_last_month 
from
(select trunc(date_trunc('month',cast(cast(s.date_id as varchar(10)) as date))) as month,
case 
when i.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when i.src_talent_description = 'RANDOM' then 'RONDA ROUSEY' 
when i.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when i.src_talent_description like '%ELIAS%' then 'ELIAS' 
when i.src_talent_description like '%BIG E%' then 'BIG E' 
when i.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else i.src_talent_description 
end as talent_description,
sum(s.net_units_sold) as units, sum(s.total_revenue) as revenue 
from {{source('fds_cpg','aggr_cpg_daily_venue_sales')}} s, {{source('fds_cpg','dim_cpg_item')}} i
where month = trunc(date_trunc('month',add_months(current_date,-2))) and s.dim_item_id = i.dim_item_id 
and s.net_units_sold > 0 and s.active_flag = 'Y'
and src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
group by 1,2)) b
on a.talent_description = b.talent_description