{{
  config({
        'schema': 'fds_sc',
		"materialized": 'view',"tags": 'Phase4A',"persist_docs": {'relation' : true, 'columns' : true},
            "post-hook" : 'grant select on {{this}} to public'
  })
}}

SELECT  dim_smprovider_account_id,
        dim_platform_id,
        dim_content_type_id,
        dim_story_id,
        dim_date_id,
        as_on_date,
        story_name,
        snap_time_posted,
        duration_hours,
        c_same_day_topsnap_views,
        c_same_day_total_time_viewed_secs,
        c_same_day_topsnap_time_viewed_secs,
        c_3day_topsnap_views,
        c_3day_total_time_viewed_secs,
        c_3day_topsnap_time_viewed_secs,
        c_7day_topsnap_views,
        c_7day_total_time_viewed_secs,
        c_7day_topsnap_time_viewed_secs,
        c_30day_topsnap_views,
        c_30day_total_time_viewed_secs,
        c_30day_topsnap_time_viewed_secs,
        'DBT_'+To_char(sysdate,'YYYY_MM_DD_HH_MI_SS')+'_SC' AS etl_batch_id,
	    'bi_dbt_user_prd' AS etl_insert_user_id,
	    sysdate etl_insert_rec_dttm, 
	    '' etl_update_user_id,
	    sysdate etl_update_rec_dttm 
FROM    {{ref('aggr_scd_daily_consumption_by_story_todate')}}      