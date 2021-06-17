/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_rydercup
   Schema	     : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to rydercup
   
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/
{% set rydercup_series_flag = ["RYDER"]%}

{{ config(materialized='ephemeral',enabled = true, tags=['domestic','tagging','rydercup'],schema='CONTENT',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_rydercup as (
select src_broadcast_orig_date,broadcast_date,broadcast_network_name,src_series_name,src_episode_title,
program_telecast_rpt_starttime,program_telecast_rpt_endtime,
src_total_duration,src_playback_period_cd,src_demographic_group,
src_genre_classification_cd,src_genre_classification_detailedtypecd,
src_program_attributes,src_daypart_cd,src_broadcast_network_service_type,
avg_audience_proj_000,avg_audience_proj_units,round(avg_audience_pct,1) as avg_audience_pct ,
avg_audience_pct_nw_cvg_area,round(share_pct) as share_pct,
round(share_pct_nw_cvg_area) as share_pct_nw_cvg_area,telecast_trackage_name,DAYNAME(broadcast_date) 
as calendardayofweekname,src_broadcast_network_id,inserted_time,
orig_broadcast_date_id,dim_nl_series_id,dim_nl_episode_id,src_telecast_id,
case 
     when src_genre_classification_cd != 'SE' then 'Shoulder'
     when telecast_trackage_name like '%REPLAY%' or telecast_trackage_name like '%HL%' 
     or src_series_name like '%HIGHLIGHTS%' or src_series_name like '%H/L%' 
     or src_program_attributes like '%(R)%' or src_series_name like '%SPECIAL%'
     then 'Shoulder'
     when src_total_duration >=60 and 
     (telecast_trackage_name not like '%REPLAY%' or telecast_trackage_name not like '%HL%' 
     or src_series_name not like '%HIGHLIGHTS%' or src_series_name not like '%H/L%' 
     or src_program_attributes not like '%(R)%' or src_series_name not like '%SPECIAL%' )
     then 'Non_Shoulder'
     else 'Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'RYDER CUP' as att_cup,
'NA' as att_season,
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',rydercup_series_flag,"","","","","")}}
,'RYDER CUP' as property
from
{{ref('base_weekly_domestic_tagging')}} where property____flag=1
)

select src_broadcast_orig_date,broadcast_date,broadcast_network_name,src_series_name,src_episode_title,
program_telecast_rpt_starttime,program_telecast_rpt_endtime,
src_total_duration,src_playback_period_cd,src_demographic_group,
src_genre_classification_cd,src_genre_classification_detailedtypecd,
src_program_attributes,src_daypart_cd,src_broadcast_network_service_type,
avg_audience_proj_000,avg_audience_proj_units,round(avg_audience_pct,1) as avg_audience_pct ,
avg_audience_pct_nw_cvg_area,round(share_pct) as share_pct,
round(share_pct_nw_cvg_area) as share_pct_nw_cvg_area,telecast_trackage_name,DAYNAME(broadcast_date) 
as calendardayofweekname,att_Shoulder,att_fights,att_cup,att_season,att_Channel_Qualifier,src_broadcast_network_id,
orig_broadcast_date_id,dim_nl_series_id,dim_nl_episode_id,src_telecast_id,
inserted_time,property  
from intm_weekly_domestic_tagging_rydercup


