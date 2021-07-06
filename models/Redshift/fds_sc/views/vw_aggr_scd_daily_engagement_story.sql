{{
  config({
        'schema': 'fds_sc',
		"materialized": 'view',"tags": 'Phase4A',"persist_docs": {'relation' : true, 'columns' : true},
            "post-hook" : 'grant select on {{this}} to public'
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
        screenshots,
        shares,
        'DBT_'+To_char(sysdate,'YYYY_MM_DD_HH_MI_SS')+'_SC' AS etl_batch_id,
	    'bi_dbt_user_prd' AS etl_insert_user_id,
	    sysdate etl_insert_rec_dttm, 
	    '' etl_update_user_id,
	    sysdate etl_update_rec_dttm 
FROM    {{ref('aggr_scd_daily_engagement_story')}} 