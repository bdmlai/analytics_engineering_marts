{{
  config({
		'schema': 'fds_kntr',"materialized": 'table','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select a.*, b.regional_viewing_hours,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id,'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from {{ref('intm_kntr_schedule_vh_data')}} a
left join {{ref('intm_kntr_region_vh')}} b on a.broadcast_month = b.broadcast_month and a.broadcast_year = b.broadcast_year
and a.region = b.region and a.demographic_type = b.demographic_type and a.demographic_group_name = b.demographic_group_name