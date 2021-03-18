
select distinct a.*, b.revenue as last_month_revenue, b.units as last_month_units, b.rank_last_month,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(select s.etl_insert_rec_dttm as created_timestamp, s.dim_shop_site_id, cast(cast(s.date_key as varchar(10)) as date),
trunc(date_trunc('month',cast(cast(s.date_key as varchar(10)) as date))) as month,
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
from 
"entdwdb"."fds_cpg"."aggr_cpg_daily_sales" s, "entdwdb"."fds_cpg"."dim_cpg_item" i, 
"entdwdb"."fds_cpg"."dim_cpg_order_method" c, "entdwdb"."fds_cpg"."dim_cpg_shop_site" ss
where s.dim_item_id = i.dim_item_id and s.dim_order_method_id = c.dim_order_method_id and 
s.dim_shop_site_id = ss.dim_shop_site_id and month = trunc(date_trunc('month',add_months(current_date,-1)))
group by s.etl_insert_rec_dttm, s.dim_shop_site_id, cast(cast(s.date_key as varchar(10)) as date), s.src_order_type,
c.src_channel_name, ss.site_name, i.src_style_id, i.src_talent_description,
i.src_style_description,s.dim_item_id) a
left join
(select *, row_number() over (order by revenue desc) as rank_last_month 
from
(select trunc(date_trunc('month',cast(cast(s.date_key as varchar(10)) as date))) as month,
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
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" s, "entdwdb"."fds_cpg"."dim_cpg_item" i
where month = trunc(date_trunc('month',add_months(current_date,-2))) and s.dim_item_id = i.dim_item_id and 
s.src_units_ordered > 0 and s.dim_shop_site_id in (1, 2, 3, 4, 5)
and src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')
group by 1,2)) b
on a.talent_description = b.talent_description