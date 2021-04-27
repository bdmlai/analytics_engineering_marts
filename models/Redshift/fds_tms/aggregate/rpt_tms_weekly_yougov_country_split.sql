{{
  config({
		"schema": 'fds_tms',
		"materialized": 'table','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
	        'post-hook': 'grant select on {{ this }} to public'
                
  })
}}
select   a.date
	,a.title
	,a.avg_appetite_score
	,a.stddev_appetite_score
	,initcap(b.src_country_name) as country
	,'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id
	,'bi_dbt_user_prd' as etl_insert_user_id
	, current_timestamp as etl_insert_rec_dttm
	, null as etl_update_user_id
	, cast(null as timestamp) as etl_update_rec_dttm
from      {{ref('intm_tms_weekly_yougov_country')}} a
inner join {{source('cdm','dim_country')}} b on a. country_code=upper(b.iso_alpha2_ctry_cd)
       and b.src_sys_cd='iso'
