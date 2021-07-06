{{
  config({
        'schema': 'fds_sc',
		"materialized": 'table',"tags": 'Phase4A',"persist_docs": {'relation' : true, 'columns' : true}
  })
}}

SELECT  dim_smprovider_account_id,
        dim_platform_id,
        dim_story_id,
        dim_content_type_id,
        dim_date_id,
        as_on_date,
        story_name,
        snap_time_posted,
        duration_hours,
        Sum(screenshots)            AS screenshots,
        Sum(shares)                 AS shares,
        'DBT_'+To_char(sysdate,'YYYY_MM_DD_HH_MI_SS')+'_SC' AS etl_batch_id,
	      'bi_dbt_user_prd' AS etl_insert_user_id,
	      sysdate etl_insert_rec_dttm, 
	      '' etl_update_user_id,
	      sysdate etl_update_rec_dttm
FROM    {{source('fds_sc','fact_scd_engagement_frame')}}
GROUP BY  1,2,3,4,5,6,7,8,9
