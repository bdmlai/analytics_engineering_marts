/*
*************************************************************************************************************************************************
   TableName   :rpt_kntr_weekly_international_tagging
   Schema	   : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy
   Description : summary table for capturing the tagged attributes of properties 
                 by performing union of all the intermediate ephemeral tables
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/


{{ config(materialized='incremental',enabled = true,tags=['international','wrestling'],
incremental_strategy='delete+insert',
unique_key= "broadcast_date||'-'||src_channel||'-'||src_country||'-'||demographic||'-'||start_time_avg_tm",
schema ='CONTENT',post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with rpt_kntr_weekly_international_tagging AS 
    (SELECT broadcast_date,
        week_0,
        src_channel,
         src_country,
        src_series,
        start_time_avg_tm,
        end_time_avg_tm,
        length_avg_tm,
        demographic,
         rat_avg_wg_pct,
        rat_num_avg_wg,
        shr_avg_wg_pct,
        "VH",
        ETL_INSERT_REC_DTTM,
        'AEW' AS property
    FROM {{ref('intm_weekly_international_tagging_aew')}}
    UNION all
    SELECT broadcast_date,
        week_0,
        src_channel,
         src_country,
        src_series,
        start_time_avg_tm,
        end_time_avg_tm,
        length_avg_tm,
        demographic,
         rat_avg_wg_pct,
        rat_num_avg_wg,
        shr_avg_wg_pct,
        "VH",
        ETL_INSERT_REC_DTTM,
        'AAA' AS property
    FROM {{ref('intm_weekly_international_tagging_aaa')}}
    UNION all
    SELECT broadcast_date,
        week_0,
        src_channel,
         src_country,
        src_series,
        start_time_avg_tm,
        end_time_avg_tm,
        length_avg_tm,
        demographic,
         rat_avg_wg_pct,
        rat_num_avg_wg,
        shr_avg_wg_pct,
        "VH",
        ETL_INSERT_REC_DTTM,
        'CMLL' AS property
    FROM {{ref('intm_weekly_international_tagging_cmll')}}
    UNION all
    SELECT broadcast_date,
        week_0,
        src_channel,
         src_country,
        src_series,
        start_time_avg_tm,
        end_time_avg_tm,
        length_avg_tm,
        demographic,
         rat_avg_wg_pct,
        rat_num_avg_wg,
        shr_avg_wg_pct,
        "VH",
        ETL_INSERT_REC_DTTM,
        'IMPACTWRESTLING' AS property
    FROM {{ref('intm_weekly_international_tagging_impactwrestling')}} )
SELECT broadcast_date,
        week_0,
        src_channel,
         src_country,
        src_series,
        start_time_avg_tm,
        end_time_avg_tm,
        length_avg_tm,
        demographic,
         rat_avg_wg_pct,
        rat_num_avg_wg,
        shr_avg_wg_pct,
        "VH",
        --ETL_INSERT_REC_DTTM,
        property,
        id,
        ROWNUMBER,
		'DBT_'||TO_CHAR(SYSDATE(),'YYYY_MM_DD_HH_MI_SS')||'_Inter' AS etl_batch_id , 'bi_dbt_user_prd' AS etl_insert_user_id , SYSDATE() AS etl_insert_rec_dttm , NULL AS etl_update_user_id , cast(null AS timestamp) AS etl_update_rec_dttm
FROM 
    (SELECT broadcast_date,
        week_0,
        src_channel,
         src_country,
        src_series,
        start_time_avg_tm,
        end_time_avg_tm,
        length_avg_tm,
        demographic,
         rat_avg_wg_pct,
        rat_num_avg_wg,
        shr_avg_wg_pct,
        "VH",
        ETL_INSERT_REC_DTTM,
        property broadcast_date||'-'||src_channel||'-'||src_country||'-'||demographic||'-'||start_time_avg_tm AS id, ROW_NUMBER()
        OVER (partition by broadcast_date,src_channel, src_country,demographic,start_time_avg_tm
    ORDER BY  ETL_INSERT_REC_DTTM desc) ROWNUMBER
    FROM rpt_kntr_weekly_international_tagging )
WHERE ROWNUMBER =1 {% if is_incremental() %}
HAVING ETL_INSERT_REC_DTTM > 
    (SELECT max(ETL_INSERT_REC_DTTM)
    FROM {{ this }}) {% endif %}