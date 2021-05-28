/*
*************************************************************************************************************************************************
   TableName   : weekly_international_tagging_aew
   Schema	     : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the international tagged data corresponding to AEW
   
   Version      Date             Author               Request
   1.0          04/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% set aew_series_1 = "%all%elite%wrestling%" %}
{% set aew_series_2 = "%aew%" %}
{% set aew_countries = ('india','south korea','spain','vietnam') %}



{{ config(materialized='ephemeral',enabled = true,tags=['international','wrestling','aew'],
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_international_tagging_aew as

(
select broadcast_date,
date_trunc('week',broadcast_date + interval '24 hours') - interval '24 hours' as week_0,
src_channel, src_country,src_series,start_time_avg_tm,end_time_avg_tm,length_avg_tm,demographic,
rat_avg_wg_pct,rat_num_avg_wg,shr_avg_wg_pct,(length_avg_tm/60)*rat_num_avg_wg as "VH",ETL_INSERT_REC_DTTM,
'AEW' as property,
{{series_ilike('src_series',aew_series_1,"1")}},
{{series_ilike('src_series',aew_series_2,"2")}},
{{international_property('src_country',aew_countries,"not in","1")}}
from  {{source('fds_kntr','fact_kntr_weekly_telecast_data')}} 
where (src_series_ilike_1 =1 or src_series_ilike_2 =1) and 
src_country_flag_1=1 and 
broadcast_date between '2017-01-01' and current_date 
)

select broadcast_date,date_trunc('week',broadcast_date + interval '24 hours') - interval '24 hours' as week_0,
src_channel, src_country,src_series,start_time_avg_tm,end_time_avg_tm,length_avg_tm,demographic,
rat_avg_wg_pct,rat_num_avg_wg,shr_avg_wg_pct,(length_avg_tm/60)*rat_num_avg_wg as "VH",ETL_INSERT_REC_DTTM,
'AEW' as property
from intm_weekly_international_tagging_aew

