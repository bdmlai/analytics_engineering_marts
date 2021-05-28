/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_mlb
   Schema	   : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to MLB
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set mlb_season_date = [('2018-03-29','2018-10-01',"Regular"),('2018-10-02','2018-10-31',"Post"),
('2019-03-20','2019-09-29',"Regular"),('2019-10-01','2019-10-30',"Post"),
('2020-07-23','2020-09-27',"Regular"),('2021-04-01','2021-10-03',"Regular"),
('2021-10-04','2021-11-03',"Post")]%}
{% set mlb_series_flag=["MLB","MLBS","NLDS"]%}
{% set mlb_not_series_flag=["COLLEGIATE","KOREAN BASEBALL"]%}
{% set mlb_genre_detailed_cd_flag = ["BBOC","BBRS","BBXP","BBOT","BBPS","BBPO","BBWS"]%}

{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','mlb'],schema='CONTENT',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_mlb as (

select *,
case 
     when broadcast_network_name not in ('MLB NETWORK','ESPN','ESPN2','ESPNEWS',
    'FOX','FOX SPORTS 1','FOX SPORTS 2','TURNER TELEVISION NETWORK','TBS NETWORK')  then 'Shoulder'
     when src_series_name in ('HOME RUN DERBY','PREVIEW','PRELUDE','MINOR','SPC','PRACTICE','SPECIAL',
     'PRE','REPEAT', 'REWIND', 'TOP 100 PROSPECTS', 'COLLEGIATE', 'FIRST YEAR PLAYER DRAFT') then 'Shoulder' 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     when src_series_name in ('RAIN D','RAIN-OUT') or
     telecast_trackage_name in ('RAIN D','RAIN-OUT') then 'Shoulder'
     else 'Non_Shoulder' end as att_Shoulder,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',mlb_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',mlb_series_flag,'src_genre_classification_detailedtypecd',
mlb_genre_detailed_cd_flag,"","or","")}},
{{property_tagging("property",'src_series_name',mlb_series_flag,
'src_series_name',mlb_not_series_flag,"","and","not")}},
'mlb' as property
from
{{ref('base_weekly_domestic_tagging')}} where property__or__flag=1 and property__and_not_flag=1
)

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
from intm_weekly_domestic_tagging_mlb



