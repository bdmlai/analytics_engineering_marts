{{
  config({
		"schema": 'fds_yt',
		"pre-hook": ["truncate fds_yt.rpt_yt_daily_wwe_talent_views_since_upload"],
		"materialized": 'incremental','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true}
  })
}}

select region_code, talent, 'since upload' as granularity,
sum(ttl_views) as total_views, sum(views_30days) as views_30days, 
count(distinct video_id) as cnt_video_id,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm

from {{ref('intm_yt_daily_wwe_talent_since_upload')}}
group by 1,2,3
order by 1 desc, 2,3 asc  