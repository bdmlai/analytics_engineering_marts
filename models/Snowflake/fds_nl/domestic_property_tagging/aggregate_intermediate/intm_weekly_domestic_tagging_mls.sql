/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_mls
   Schema	   : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to MLS
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set mls_shoulder_series_name =['BARRA','COPA','ESPN FC','FOX','JUEGO','PREMIACION','RAPIDS','SOMOS']%}
{% set mls_shoulder_broadcast_network_name =['GALAVISION','TF','UNI']%}

{% set mls_season_date = [('2018-03-03','2018-10-28',"Regular"),
('2019-03-02','2019-10-06' ,"Regular"),
('2020-02-29','2020-11-08',"Regular"),('2021-04-17','2021-11-07',"Regular"),
('2018-10-31','2018-12-08',"Post"),
('2019-10-19','2019-11-10' ,"Post"),
('2020-11-20','2020-12-12',"Post"),('2021-11-19','2021-12-11' ,"Post")]%}
 
{% set mls_series_flag=['MLS','MAJOR LEAGUE SOCCER','US OPEN CUP SOCCER','MAJOR LEAG SOCCER' ]%}
{% set mls_genre_detailed_cd_flag = ['SOCC','SOCH','SOMS','SOPL','SOOC','SPHL','N/A']%}

{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','mls'],
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_mls as (
select *,
case 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     {% for i in mls_shoulder_series_name %}
     when src_series_name like '%{{i}}%'  then 'Shoulder'
     {% endfor %} 
     {% for i in mls_shoulder_broadcast_network_name %}
     when broadcast_network_name like '%{{i}}%'  then 'Non_Shoulder'
     {% endfor %} 
     else 'Non_Shoulder' end as att_Shoulder,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',mls_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',mls_series_flag,'src_genre_classification_detailedtypecd',
mls_genre_detailed_cd_flag,"","and","")}},'mls' as property
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
from intm_weekly_domestic_tagging_mls