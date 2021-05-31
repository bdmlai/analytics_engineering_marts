/*
*************************************************************************************************************************************************
   TableName   : base_weekly_domestic_tagging
   Schema	   : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy
   Description : Ephemeral base table formed by performing join on the 5 source tables
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/


{{ config(materialized='ephemeral',enabled = true, tags=['domestic','tagging','base'],
          post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with base_weekly_domestic_tagging as (
select src_broadcast_orig_date,broadcast_date,broadcast_network_name,src_series_name,src_episode_title,
program_telecast_rpt_starttime,program_telecast_rpt_endtime,
src_total_duration,src_playback_period_cd,src_demographic_group,t5.src_genre_classification_cd,t5.src_genre_classification_detailedtypecd,
src_program_attributes,src_daypart_cd,src_broadcast_network_service_type,avg_audience_proj_000,avg_audience_proj_units,round(avg_audience_pct,1) as avg_audience_pct ,
avg_audience_pct_nw_cvg_area,round(share_pct) as share_pct,round(share_pct_nw_cvg_area) as share_pct_nw_cvg_area,telecast_trackage_name,
DAYNAME(broadcast_date) as calendardayofweekname,t1.src_broadcast_network_id,t1.ETL_INSERT_REC_DTTM as inserted_time
from {{source('pt_fds_nl','fact_nl_program_viewership_ratings')}} t1
left join {{source('pt_fds_nl','dim_nl_series')}}  t2 on t1.dim_nl_series_id = t2.dim_nl_series_id and t1.dim_source_sys_id = t2.dim_source_sys_id
left join {{source('pt_fds_nl','dim_nl_episode')}} t3 on t1.dim_nl_episode_id = t3.dim_nl_episode_id and t1.dim_source_sys_id = t3.dim_source_sys_id
left join {{source('pt_fds_nl','dim_nl_broadcast_network')}} t4 on t1.dim_nl_broadcast_network_id = t4.dim_nl_broadcast_network_id and t1.dim_source_sys_id = t4.dim_source_sys_id
left join {{source('pt_fds_nl','dim_nl_genre')}} t5 on t1.dim_nl_genre_id = t5.dim_nl_genre_id and t1.dim_source_sys_id = t5.dim_source_sys_id
where src_broadcast_orig_date between '2018-01-01'and current_date
and src_demographic_group in ('P2-999') and src_playback_period_cd = 'Live+SD')

select * from base_weekly_domestic_tagging