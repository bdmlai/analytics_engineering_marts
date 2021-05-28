/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_bundesliga
   Schema	   : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to bundesliga
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set bundesliga_shoulder_broadcast_network_name =['TUDN','UNIVISION DEPORTES']%}

{% set bundesliga_season_date = [('2018-08-24','2019-05-18',"Regular"),('2019-08-16','2020-06-27' ,"Regular"),
('2020-09-18','2021-05-15',"Regular")]%}
 
{% set bundesliga_series_flag=['BUNDESLIGA']%}
{% set bundesliga_genre_detailed_cd_flag = ['SOCC','SOIS']%}

{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','bundesliga'],schema='CONTENT',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_bundesliga as (
select *,
case 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     when src_total_duration <= 118 then 'Shoulder'
     {% for i in bundesliga_shoulder_broadcast_network_name %}
     when broadcast_network_name like '%{{i}}%'  then 'Non_Shoulder'
     {% endfor %} 
     else 'Non_Shoulder' end as att_Shoulder,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',bundesliga_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',bundesliga_series_flag,'src_genre_classification_detailedtypecd',
bundesliga_genre_detailed_cd_flag,"","and","")}},'bundesliga' as property
from {{ref('base_weekly_domestic_tagging')}} where property__and__flag=1 )

select src_broadcast_orig_date,broadcast_date,broadcast_network_name,src_series_name,src_episode_title,
program_telecast_rpt_starttime,program_telecast_rpt_endtime,
src_total_duration,src_playback_period_cd,src_demographic_group,
src_genre_classification_cd,src_genre_classification_detailedtypecd,
src_program_attributes,src_daypart_cd,src_broadcast_network_service_type,
avg_audience_proj_000,avg_audience_proj_units,round(avg_audience_pct,1) as avg_audience_pct ,
avg_audience_pct_nw_cvg_area,round(share_pct) as share_pct,
round(share_pct_nw_cvg_area) as share_pct_nw_cvg_area,telecast_trackage_name,DAYNAME(broadcast_date) 
as calendardayofweekname,att_Shoulder,att_cup,att_season,att_Channel_Qualifier,src_broadcast_network_id,
inserted_time,property 
from intm_weekly_domestic_tagging_bundesliga



