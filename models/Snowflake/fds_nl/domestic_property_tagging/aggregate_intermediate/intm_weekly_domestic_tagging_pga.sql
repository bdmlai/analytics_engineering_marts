/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_pga
   Schema	     : fds_nl
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to pga
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set pga_series_flag = [ 'PGA TOUR','PGAT', 'PGA CUP', 'PGA OF AMERICA', 'PGA CHAMP'] %}
{% set pga_series_not_flag = ["SENIOR", "WOMEN","PGA JR"] %}

{% set pga_season_date = [('2018-10-4','2019-08-25',"Regular"),
('2019-09-12','2020-08-30',"Regular"),('2020-09-10','2021-09-05',"Regular")]%}


{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','pga'],schema= 'fds_nl',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_pga as (
select *,
case 
     when src_total_duration <=30 then 'Shoulder'
     when src_genre_classification_detailedtypecd != 'SE' then 'Shoulder'
     when src_series_name like '%CV' or src_series_name like '%CXL' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_program_attributes not like '%(R)%' and src_total_duration >= 180  then 'Non_Shoulder'
     when src_program_attributes not like '%(L)%' and
    (src_series_name like '%THE CUT%' or src_series_name like '%INSIDE%' or 
     src_series_name like '%B/R ON THE TEE%' or
     src_series_name like '%PREVIEW%' or src_series_name like '%H/L%' or
     src_series_name like '%SPECIAL%' or src_series_name like '%BONUS%' or
     src_series_name like '%HIGHLIGHT%' or src_series_name like '%CLASSIC%'
     or src_episode_title like '%CLASSIC%') then 'Shoulder'
     when src_episode_title like 'N/A' and src_total_duration < 180 then 'Shoulder'
     when src_genre_classification_detailedtypecd in ('BBOC','GOLC','GOLF','GOOT') then 'Shoulder'
     else 'Non_Shoulder'
end as att_Shoulder,
'NA' as att_fights,
'NA' as att_cup,
{{season_tagging("season",'broadcast_date',pga_season_date)}},
'NA' as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',pga_series_flag,
'src_series_name',pga_series_not_flag,"","and","not")}},
'PGA' as property
from {{ref('base_weekly_domestic_tagging')}}
where property__and_not_flag = 1 and src_series_name not like '%LPGA%'
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
from intm_weekly_domestic_tagging_pga

