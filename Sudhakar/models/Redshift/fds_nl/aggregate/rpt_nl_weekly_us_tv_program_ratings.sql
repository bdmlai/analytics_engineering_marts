{{
  config({
	    'schema': 'fds_nl',       
	    "materialized": 'table',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true},
            'post-hook': 'grant select on fds_nl.rpt_nl_weekly_us_tv_program_ratings to public'			
  })
}}

select week,year,program_type,src_demographic_group,src_playback_period_cd,
 avg_audience_proj_000, avg_audience_pct,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(
select week,year,program_type,src_demographic_group,
       src_playback_period_cd, avg_audience_proj_000, avg_audience_pct 
from {{ref('intm_nl_weekly_wwe_programs')}}

union all
select week,year,program_type,src_demographic_group,
       src_playback_period_cd, avg_audience_proj_000, avg_audience_pct  
from  {{ref('intm_nl_weekly_aew_programs')}}
)
