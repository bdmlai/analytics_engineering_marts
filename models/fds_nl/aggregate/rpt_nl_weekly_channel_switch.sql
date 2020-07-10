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
		"pre-hook": ["delete from fds_nl.rpt_nl_weekly_channel_switch where etl_insert_rec_dttm > (select max(etl_insert_rec_dttm) from fds_nl.fact_nl_weekly_live_switching_behavior_destination_dist)"],
		"materialized": 'incremental','tags': 'Phase 4b', "persist_docs": {'relation' : true, 'columns' : true}
  })
}}

select a.broadcast_Date,a.coverage_area,a.src_market_break,
a.src_broadcast_network_name,a.src_demographic_group,a.time_minute,
most_current_us_audience_avg_proj_000 as mc_us_aa000,
absolute_set_off_off_air,
absolute_stay,stay_percent,absolute_switch,switch_percent,
dense_rank() over(partition by src_broadcast_network_name,broadcast_Date,src_demographic_group  order by switch_percent desc NULLS LAST)
as switch_percent_rank,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' AS etl_batch_id,
    'bi_dbt_user_uat'                                   AS etl_insert_user_id,
    CURRENT_TIMESTAMP                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
 from {{ref('intermediate_nl_absolute_switch_stay_detail')}}  A 
