-- Switch behavior absolute value ranking table

/*
*************************************************************************************************************************************************
   Date        : 06/19/2020
   Version     : 1.0
   TableName   : rpt_nl_weekly_channel_switch
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : rpt_nl_weekly_channel_switch view consists of absolute stay ,absolute switch and ranking based on switch for WWE, AEW and other wrestling programs (Weekly)
*************************************************************************************************************************************************
*/

{{
  config({
		"schema": 'fds_nl',
		"pre-hook": ["truncate fds_nl.rpt_nl_weekly_channel_switch"],
		"materialized": 'incremental','tags': 'Phase4B', "persist_docs": {'relation' : true, 'columns' : true}
  })
}}

select a.broadcast_Date,a.coverage_area,a.src_market_break,
a.src_broadcast_network_name,a.src_demographic_group,a.time_minute,
most_current_us_audience_avg_proj_000 as mc_us_aa000,
absolute_set_off_off_air,
absolute_stay,stay_percent,absolute_switch,switch_percent,
 b.switch_percent_rank as switch_percent_rank,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' AS etl_batch_id,
    'bi_dbt_user_prd'                                   AS etl_insert_user_id,
    CURRENT_TIMESTAMP                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
 from {{ref('intermediate_nl_absolute_switch_stay_detail')}}  A 
left  join {{ref('intermediate_nl_ranking')}}  b
 on a.broadcast_Date = b.broadcast_Date and
 a.src_broadcast_network_name = b.src_broadcast_network_name and
 a.src_demographic_group = b.src_demographic_group and 
 a.time_minute = b.time_minute 