/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_open_championship
   Schema	     : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to open_championship
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set open_championship_series_flag = ["OPEN CHAMPIONSHIP","R&A OPEN CHP"] %}
{% set open_championship_series_not_flag = ["SENIOR OPEN CHAMPIONSHIP"] %}

{% set open_championship_season_date = [('2018-07-19','2018-07-22',"Regular"),
('2019-07-18','2019-07-21',"Regular"),('2021-07-16','2021-07-19',"Regular")]%}


{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','open_championship'],schema= 'CONTENT',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_open_championship as (
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
     when src_genre_classification_cd != 'SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_program_attributes not like '%(R)%' and src_total_duration >= 120  then 'Non_Shoulder'
     when src_program_attributes like '%(L)%' and src_series_name not like '%REPEAT%' 
     and src_series_name not like '%RPT%' and src_series_name not like '%CLASSIC%' then 'Non_Shoulder'
     when src_series_name like '%H/L%' then 'Shoulder'
     else 'Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',open_championship_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',open_championship_series_flag,
'src_series_name',open_championship_series_not_flag,"","and","not")}},
'OPEN CHAMPIONSHIP' as property
from {{ref('base_weekly_domestic_tagging')}}
where property__and_not_flag = 1 


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
from intm_weekly_domestic_tagging_open_championship

