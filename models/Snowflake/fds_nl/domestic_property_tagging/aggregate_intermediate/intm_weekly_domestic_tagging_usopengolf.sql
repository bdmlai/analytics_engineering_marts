/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_usopengolf
   Schema	     : fds_nl
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to usopengolf
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set usopengolf_series_flag = ["US OPEN","U.S. OPEN"] %}
{% set usopengolf_genre_detailed_cd_flag = ["GOPR","GOLP", "GOLF","GOLC" ] %}

{% set usopengolf_season_date = [('2018-06-14','2018-06-17',"Regular"),
('2019-06-13','2019-06-16',"Regular"),('2020-09-17','2020-09-20',"Regular"),
('2021-06-17','2021-06-20',"Regular")]%}


{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','usopengolf'],schema= 'fds_nl',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_usopengolf as (
select *,
case 
     when src_episode_title like 'N/A' then 'Shoulder'
     when src_genre_classification_detailedtypecd != 'SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_program_attributes not like '%(R)%' and src_total_duration >= 150  then 'Non_Shoulder'
     when src_series_name like '%H/L%' or src_series_name like '%HIGHLIGHT%' or
     src_series_name like '%SPECIAL%' or src_series_name like '%EPICS%' then 'Shoulder'
     else 'Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',usopengolf_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',usopengolf_series_flag,
'src_genre_classification_detailedtypecd',usopengolf_genre_detailed_cd_flag,"","and","")}},
'US OPEN GOLF' as property
from {{ref('base_weekly_domestic_tagging')}}
where property__and__flag = 1 


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
inserted_time,property 
from intm_weekly_domestic_tagging_usopengolf

