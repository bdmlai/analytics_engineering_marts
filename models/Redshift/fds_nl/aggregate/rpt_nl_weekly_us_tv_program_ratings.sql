{{
  config({
	    'schema': 'fds_nl',       
	    "materialized": 'table',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true},
            'post-hook': 'grant select on {{ this }} to public'			
  })
}}

SELECT 	broadcast_date,
		week,
        year,
        week_number,
        program_type,
        src_demographic_group,
        src_playback_period_cd,
        avg_audience_proj_000,
        avg_audience_pct,
        'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' AS etl_batch_id,
        'bi_dbt_user_prd' AS etl_insert_user_id,
        current_timestamp AS etl_insert_rec_dttm,
        null AS etl_update_user_id,
        cast(null as timestamp) AS etl_update_rec_dttm
FROM
(
        SELECT  broadcast_date,
                week,
                year,
                week_number,
                program_type,
                src_demographic_group,
                src_playback_period_cd, 
                avg_audience_proj_000, 
                avg_audience_pct 
        FROM    {{ref('intm_nl_weekly_wwe_programs')}}
        UNION ALL
        SELECT  broadcast_date,
                week,
                year,
                week_number,
                program_type,
                src_demographic_group,
                src_playback_period_cd, 
                avg_audience_proj_000, 
                avg_audience_pct  
        FROM    {{ref('intm_nl_weekly_aew_programs')}}
)