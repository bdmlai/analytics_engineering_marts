{{
  config({
		'schema': 'fds_le',
		"materialized": 'incremental',"tags": 'Phase 5a',"persist_docs": {'relation' : true, 'columns' : true}
  })
}}


select *,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_LE' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
from (
select venue_city,venue_city as cityname,venue_nm,brand_nm,event_Type_cd,event_date,days_to_event,
'Actual' as comp_type,
event_capacity,gross,paid,round(util,2) as util,
 null as fgross, null as fpaid, null as futil ,event_date as maxeventdate,1 as orders
 from {{ref('intm_le_advances_ticket_combine')}}
union all
select venue_city,'' as cityname,bvenuename as venue_nm,'' as brand_nm,'' as event_Type_cd,beventdate as event_date ,0 as days_to_event,
'Best Comp' as comp_type, 
bcapacity as event_capacity,bsgross as gross,bspaid as paid,round(bsutil,2) as util, 
bgross as fgross, bpaid as fpaid, round(butil,2) as futil ,event_date as maxeventdate,2 as orders
from {{ref('intm_le_advances_ticket_combine')}}
union all 
select venue_city,'' as cityname,'' as venue_nm,'' as brand_nm,'' as event_Type_cd,null as event_date ,0 as days_to_event,
'Average of Comps'||'('||CONVERT(varchar(2), nvl(ano_event,'0'))||')' as comp_type,
ascapacity as event_capacity,asgross as gross,aspaid as paid,round(asutil,2) as util, 
agross as fgross, apaid as fpaid, round(autil,2) as futil ,event_date as maxeventdate,3 as orders
from {{ref('intm_le_advances_ticket_combine')}}
order by maxeventdate,venue_city, orders
)