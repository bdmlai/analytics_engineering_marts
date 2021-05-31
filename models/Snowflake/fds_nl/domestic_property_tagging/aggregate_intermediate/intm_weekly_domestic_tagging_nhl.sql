/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_nhl
   Schema	   : fds_nl
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to NHL
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set nhl_series_flag=["NHL","HOCKEY DAY"]%}
{%set nhl_genre_detailed_cd_flag = ["NHOT","NHRS","NHOC","NHCP","NHXP","NHOA"] %}
{% set nhl_season_date = [('2018-10-03', '2019-04-06',"Regular"),
('2019-04-10','2019-06-12',"Post"),
('2019-10-02' ,'2020-03-11',"Regular"),('2020-08-01','2020-09-28',"Post"),
('2021-01-13' ,'2021-04-30',"Regular"),('2021-05-01' ,'2021-07-31',"Post")]%}


{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','nhl'],schema='fds_nl',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}
with intm_weekly_domestic_tagging_nhl as (
select *,
case 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     when src_series_name like '%BONUS%' and src_total_duration < 30 then 'Shoulder'
     when (src_genre_classification_detailedtypecd in ('NHOT') and
     src_series_name not in ('STAR')) then 'Shoulder' 
     when src_series_name in ('ENCORE','LIVE PRE','LIVE POST','SUPERSKILLS','LIVE SPECIAL',
     'WORLD CHAMPIONSHIP','POST','PRESHOW','FANTASY','DRAFT')  then 'Shoulder' 
     else 'Non_Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',nhl_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',nhl_series_flag,
'src_genre_classification_detailedtypecd',nhl_genre_detailed_cd_flag,"","and","")}}
,'NHL' as property
from
{{ref('base_weekly_domestic_tagging')}} where property__and__flag=1
 )

select src_broadcast_orig_date,broadcast_date,broadcast_network_name,src_series_name,src_episode_title,
program_telecast_rpt_starttime,program_telecast_rpt_endtime,
src_total_duration,src_playback_period_cd,src_demographic_group,
src_genre_classification_cd,src_genre_classification_detailedtypecd,
src_program_attributes,src_daypart_cd,src_broadcast_network_service_type,
avg_audience_proj_000,avg_audience_proj_units,round(avg_audience_pct,1) as avg_audience_pct ,
avg_audience_pct_nw_cvg_area,round(share_pct) as share_pct,
round(share_pct_nw_cvg_area) as share_pct_nw_cvg_area,telecast_trackage_name,DAYNAME(broadcast_date) 
as calendardayofweekname,att_Shoulder,att_fights,att_cup,att_season,att_Channel_Qualifier,
src_broadcast_network_id,inserted_time,property
from intm_weekly_domestic_tagging_nhl