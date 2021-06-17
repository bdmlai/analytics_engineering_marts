/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_uot
   Schema	   : fds_nl
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to uot
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set uot_series_flag=["US OPEN"]%}
{% set uot_genre_detailed_cd_flag = ["TEOT","TEPR"] %}
{% set uot_broadcast_network_name = ["ESPN","TENNIS CHANNEL"] %}


{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','uot'],schema='fds_nl',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_uot as (
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
     when src_program_attributes like '%R%'  then 'Shoulder'
     when broadcast_network_name in ('TENNIS CHANNEL') then 'Shoulder'
     when src_episode_title = 'N/A' and src_program_attributes not like '%(L)%' then 'Shoulder'
     when src_episode_title like '%PREVIEW%' then 'Shoulder'
     when src_series_name like '%TENNIS%' and  src_series_name like '%US OPEN%'
     and src_program_attributes like '%(L)%' and src_total_duration>=80 then 'Non_Shoulder'
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_episode_title like '%REPEAT%' or src_series_name like '%CLASSIC%' then 'Shoulder'
     else 'Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
'NA' as att_Season,
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',uot_series_flag,
'src_genre_classification_detailedtypecd',uot_genre_detailed_cd_flag,"","and","")}},
{{property_tagging("property",'broadcast_network_name',uot_broadcast_network_name,"","","","","")}}
,'US OPEN TENNIS' as property 
from
{{ref('base_weekly_domestic_tagging')}} where property__and__flag=1 and property____flag=1
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
from intm_weekly_domestic_tagging_uot