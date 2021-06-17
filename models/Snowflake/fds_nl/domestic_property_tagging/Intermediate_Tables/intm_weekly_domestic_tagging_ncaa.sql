/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_ncaa
   Schema	     : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to NCAA
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set ncaa_series_flag = ["NCAA"] %}
{% set ncaa_genre_detailed_cd_flag = ["CBKC"] %}
{% set ncaa_series_not_flag = ["WMNS","WOMEN"] %}


{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','ncaa'],schema= 'CONTENT',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_ncaa as (
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
     when src_episode_title like 'N/A' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' or 
     src_series_name like '%CLASSIC%' then 'Shoulder'
     when src_total_duration <= 100 then 'Shoulder'
     else 'Non_Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
'NA' as att_Season,
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',ncaa_series_not_flag,"","","not","","")}},
{{property_tagging("property",'src_series_name',ncaa_series_flag,
'src_genre_classification_detailedtypecd',ncaa_genre_detailed_cd_flag,"","and","")}},
'NCAA' as property
from {{ref('base_weekly_domestic_tagging')}}
where property_not___flag=1 and property__and__flag =1


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
from intm_weekly_domestic_tagging_ncaa



