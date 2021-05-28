/*
*************************************************************************************************************************************************
   TableName   : intm_weekly_domestic_tagging_nascar
   Schema	     : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the tagged data corresponding to NASCAR
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set nascar_series_flag = ["NASCAR","NSCR","NGOTS","DAYTONA","DYTNA","ARCA RACING","NGROTS"] %}
{% set nascar_genre_detailed_cd_flag = ["MSNS","MSNA","MSNB","MSNT"] %}
{% set nascar_series_not_flag = ["ANIMAL"] %}

{% set nascar_cup = [("MNSTR ENRGY","M.E Cup"),("MONSTER ENERGY","M.E Cup"),("M.E. CP","M.E Cup"),
("M.E. CUP","M.E Cup"),("NASCAR CUP SERIES","M.E Cup"),("NASCAR SERIES","M.E Cup"),
("XFINITY","Xfinity Srs"),("NASCAR XFIN","Xfinity Srs"),("XFININTY","Xfinity Srs"),("XFNTY","Xfinity Srs"),
("NASCAR XF","Xfinity Srs"),("DAYTONA 500","DAYTONA Cup"),("DYTONA 500","DAYTONA Cup"),
("WHELEN","Nascar Minor Series"),("NASCAR K&N","Nascar Minor Series"),
("SHOOT-OUT","Nascar Minor Series"),("NCWTS","Nascar Minor Series"),("NSCR TRUCK","Nascar Minor Series"),
("NASCAR TRUCK","Nascar Minor Series"),("NGOTS","Nascar Minor Series"),("NGROTS","Nascar Minor Series"),
("ARCA RACING","ARCA")]%}  



{{ config(materialized='ephemeral',enabled = true,tags=['domestic','tagging','nascar'],schema= 'CONTENT',
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_domestic_tagging_nascar as (
select *,
case 
     when src_genre_classification_cd !='SE' then 'Shoulder'
     when src_program_attributes like '%(R)%' then 'Shoulder'
     when src_series_name like '%REPEAT%' or src_series_name like '%RPT%' then 'Shoulder'
     when src_total_duration <= 100 then 'Shoulder'
     when UPPER(src_series_name) like '%HOT PASS%' then 'Shoulder'
     else 'Non_Shoulder'
end as att_Shoulder,
case
{% for i,j in nascar_cup %}
when src_series_name like '%{{i}}%' then '{{j}}' 
{% endfor %}
else 'NA'
end as att_cup,
'NA' as att_Season,
case 
     when UPPER(broadcast_network_name) like '%DEPORTES%' then 'DEPORTES'
     else 'NA'
end as att_Channel_Qualifier,
{{property_tagging("property",'src_series_name',nascar_series_flag,
'src_series_name',nascar_series_not_flag,"","and","not")}},
{{property_tagging("property",'src_series_name',nascar_series_flag,
'src_genre_classification_detailedtypecd',nascar_genre_detailed_cd_flag,"","or","")}},
'nascar' as property
from {{ref('base_weekly_domestic_tagging')}}
where property__and_not_flag = 1 and property__or__flag =1


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
from intm_weekly_domestic_tagging_nascar

