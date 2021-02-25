{{
  config({
		"schema": 'fds_le',
		"materialized": 'incremental','tags': 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select a.*, c.state_covid_growth_rank,
coalesce(b.large_gatherings_ban_bucket_group, a.large_gatherings_ban_bucket_group) as large_gatherings_ban_previous,
(b.state_regulation_update_date_max + 1) as large_gatherings_change_date,
coalesce(d.face_covering_requirement_bucket_group, a.face_covering_requirement_bucket_group) as face_covering_requirement_previous,
(d.state_regulation_update_date_max + 1) as face_covering_requirement_change_date,
('DBT_' || TO_CHAR(((convert_timezone ('UTC', getdate()))::timestamp),'YYYY_MM_DD_HH24_MI_SS') || '_5A')::varchar(255) as etl_batch_id, 
'SVC_PROD_BI_DBT_USER' as etl_insert_user_id, ((convert_timezone ('UTC', getdate()))::timestamp) as etl_insert_rec_dttm, 
null::varchar(255) as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from {{ref('intm_le_daily_routing_state_data')}} a 
left join {{ref('intm_le_regulation_change_gathering')}} b on trim(lower(a.state)) = trim(lower(b.state))
left join {{ref('intm_le_routing_state_covid_growth')}} c on trim(lower(a.state)) = trim(lower(c.state))
left join {{ref('intm_le_regulation_change_facecovering')}} d on trim(lower(a.state)) = trim(lower(d.state))