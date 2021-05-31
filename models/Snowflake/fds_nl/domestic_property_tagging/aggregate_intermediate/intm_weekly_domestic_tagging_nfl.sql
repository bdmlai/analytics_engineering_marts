/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_nfl
   Schema	   : fds_nl
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to NFL
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set nfl_series_flag = ["NFL","AFC","NFC","FULL GAME BROADCAST",
"HALL OF FAME GAME","MONDAY NIGHT FOOTBALL","SUPER BOWL","SUNDAY NIGHT FOOTBALL",
"THURSDAY NIGHT FOOTBALL","NFLN THU NT","NFL EN UNVSO","MONDAY NIGHT FTBL"] %}
{% set nfl_broadcast_network_flag = ["NBC","CBS","FOX","NFL NETWORK",
"FOX DEPORTES","ESPN","ESPN DEPORTES","ESPN2",
"SHOWTIME PRIME","ABC","UNIVERSO"] %}
{% set nfl_season_date = [('2018-09-06','2018-12-30',"Regular"),
('2019-01-05','2019-02-03',"Post"),
('2019-09-05','2019-12-29',"Regular"),('2020-01-04','2020-02-02',"Post"),
('2020-09-10','2021-01-03',"Regular"),('2021-01-09','2021-02-07',"Post")]%}

{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','nfl'],schema= 'fds_nl',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_nfl as (

select *,
case 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     when src_total_duration <= 130 then 'Shoulder'
     when calendardayofweekname in ('Tue','Wed') then 'Shoulder'
     when src_series_name in ('DRAFT', 'SCOUTING', 'COLLEGIATE', 'OPENING NIGHT',
     'ESPN NFL','FULL GAME BROADCAST')  then 'Shoulder'
     when broadcast_network_name in ('NFL NETWORK') and  src_program_attributes not like '%(L)%' then 'Shoulder'
     when broadcast_network_name in ('UNIVERSO') and  src_series_name like '%NFL EN%' then 'Shoulder'
     else 'Non_Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',nfl_season_date)}},'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',nfl_series_flag,
'broadcast_network_name',nfl_broadcast_network_flag,"","and","")}},
'NFL' as property from
{{ref('base_weekly_domestic_tagging')}} where property__and__flag=1)

select src_broadcast_orig_date,broadcast_date,broadcast_network_name,src_series_name,src_episode_title,
program_telecast_rpt_starttime,program_telecast_rpt_endtime,
src_total_duration,src_playback_period_cd,src_demographic_group,
src_genre_classification_cd,src_genre_classification_detailedtypecd,
src_program_attributes,src_daypart_cd,src_broadcast_network_service_type,
avg_audience_proj_000,avg_audience_proj_units,round(avg_audience_pct,1) as avg_audience_pct ,
avg_audience_pct_nw_cvg_area,round(share_pct) as share_pct,
round(share_pct_nw_cvg_area) as share_pct_nw_cvg_area,telecast_trackage_name,DAYNAME(broadcast_date) 
as calendardayofweekname,att_Shoulder,att_fights,att_cup,att_season,att_Channel_Qualifier,src_broadcast_network_id,
inserted_time,property 
from intm_weekly_domestic_tagging_nfl