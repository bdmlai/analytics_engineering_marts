/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_nba
   Schema	   : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to NBA
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set nba_series_flag=["NBA","BASKETBALL HALL OF FAME"]%}
{% set nba_not_series_flag=["WNBA"] %}
{% set nba_genre_detailed_cd_flag = ["BKXP","BKRS","BKOC","BKOT","BKPO","BKCH","BKAM","BKOA"] %}
{% set nba_not_genre_detailed_cd_flag = ["BBXP","CBOC","BKAM","FF","CS","UN PV"] %}
{% set nba_season_date = [('2018-10-16','2019-04-10',"Regular"),
('2019-04-13','2019-06-13',"Post"),
('2019-10-22','2020-03-11',"Regular"),('2020-07-30','2020-08-14',"Regular"),
('2020-08-15','2020-10-11',"Post"),('2020-12-22','2021-05-16',"Regular"),
('2021-05-22','2021-07-22',"Post")]%}
{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','nba'],schema='CONTENT',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_nba as (
select *,
case 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     when src_total_duration <= 120 then 'Shoulder'
     when broadcast_network_name not in ('ABC', 'ESPN', 'ESPN2', 'NBA-TV', 'TURNER NETWORK TELEVISION') then 'Shoulder'
     when src_series_name in ('D LEAGUE','PRE-DRAFT','KICKSTART','CLASSICS','WNBA','JUNIOR','G-LEAGUE',
     'REPEAT','RPT','WOMEN','COAST TO COAST','RISING','ROOKIE','NBA 2K')  then 'Shoulder'
     else 'Non_Shoulder' end as att_Shoulder,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',nba_season_date)}},'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',nba_series_flag,
'src_genre_classification_detailedtypecd',nba_genre_detailed_cd_flag,"","or","")}},
{{property_tagging("property",'src_series_name',nba_not_series_flag,
'src_series_name',nba_series_flag,"not","and","")}},
{{property_tagging("property",'src_series_name',nba_series_flag,
'src_genre_classification_detailedtypecd',nba_not_genre_detailed_cd_flag,"","and","not")}},
'nba' as property
from {{ref('base_weekly_domestic_tagging')}} where property__or__flag =1 and property_not_and__flag =1
and property__and_not_flag =1
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
 from intm_weekly_domestic_tagging_nba