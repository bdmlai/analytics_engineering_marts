{{
  config({
		"schema": 'fds_nplus',               
		"materialized": 'incremental','tags': "rpt_nplus_daily_milestone_complete_rates", "persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": ['grant select on {{this}} to public',
		'drop table dt_prod_support.intm_nplus_viewershipdata_with_externalid_live',
		'drop table dt_prod_support.intm_nplus_event_litelog_live']
  })
}}
select    type,
	  external_id,
	  title,
	  cast(premiere_date as date) premiere_date,
	  complete_rate,
	  viewers_count,
	  'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
          'bi_dbt_user_prd' as etl_insert_user_id, 
           current_timestamp as etl_insert_rec_dttm, 
          null as etl_update_user_id,
          cast(null as timestamp) as etl_update_rec_dttm
 from
( 
select type,external_id,title,
        premiere_date,complete_rate,viewers_count
from {{ref('intm_nplus_daily_milestone_complete_rates_ppv')}}

union all

select type,external_id,title,
        premiere_date,complete_rate,viewers_count
from {{ref('intm_nplus_daily_milestone_complete_rates_nxt_tko')}}

union all

select type,external_id,title,
        premiere_date,complete_rate,viewers_count
from {{ref('intm_nplus_daily_milestone_complete_rates_live')}}


)