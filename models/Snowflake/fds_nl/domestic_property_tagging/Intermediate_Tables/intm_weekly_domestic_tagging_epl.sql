/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_epl
   Schema	   : fds_nl
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to EPL
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set epl_non_shoulder_broadcast_network_name =["BRAVO","E!","ESQUIRE NETWORK","MSNBC","OXYGEN MEDIA","SYFY"]%}
{% set epl_shoulder_series_name =["BPL PREGAME SHOW","EPL PREVIEW SHOW L"]%}
{% set epl_shoulder_genre_detailed_cd =["SPHL","SOOC"]%}

{% set epl_season_date = [('2018-08-11','2019-05-19',"Regular"),
('2019-08-09','2020-07-26',"Regular"),
('2020-09-12','2021-05-23',"Regular")]%}

{% set epl_series_flag=["PREMIER LGE","PL DEADLINE","PREMIER LEAGUE","PREMIER LG","PL MATCH"]%}
{% set epl_not_series_flag=["PREMIER LEAGUE DARTS"]%}
{% set epl_genre_detailed_cd_flag = ["SOCC","SPOT","SOOC"]%}

{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','epl'],schema='fds_nl',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_epl as (

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
     {% for i in epl_non_shoulder_broadcast_network_name %}
     when broadcast_network_name like '%{{i}}%'  then 'Non_Shoulder'
     {% endfor %} 
     {% for i in epl_shoulder_series_name %}
     when src_series_name like '%{{i}}%'  then 'Shoulder'
     {% endfor %} 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_genre_classification_detailedtypecd like 'SOOC' then 'Shoulder'
     else 'Non_Shoulder' end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',epl_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',epl_series_flag,'src_genre_classification_detailedtypecd',
epl_genre_detailed_cd_flag,"","and","")}},
{{property_tagging("property",'src_series_name',epl_series_flag,
'src_series_name',epl_not_series_flag,"","and","not")}},
'EPL' as property
from
{{ref('base_weekly_domestic_tagging')}} where property__and__flag=1 and property__and_not_flag=1
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
from intm_weekly_domestic_tagging_epl
