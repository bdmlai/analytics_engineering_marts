/*
*************************************************************************************************************************************************
   TableName   : weekly_international_tagging_impactwrestling
   Schema	   : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Intermediate Ephemeral table for capturing the international tagged data corresponding to AAA
   
   Version      Date             Author               Request
   1.0          04/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/


{% set impactwrestling_series_1 = "%impact%wrestling%" %}

{{ config(materialized='ephemeral',enabled = true,tags=['international','wrestling','impactwrestling'],
post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with intm_weekly_international_tagging_impactwrestling as

(
select broadcast_date,date_trunc('week',broadcast_date + interval '24 hours') - interval '24 hours' as week_0,
src_channel, src_country,src_series,start_time_avg_tm,end_time_avg_tm,length_avg_tm,demographic,
rat_avg_wg_pct,rat_num_avg_wg,shr_avg_wg_pct,(length_avg_tm/60)*rat_num_avg_wg as "VH",ETL_INSERT_REC_DTTM,
'IMPACTWRESTLING' as property,
{{series_ilike('src_series',impactwrestling_series_1,"1")}}
from {{source('sf_fds_kntr','fact_kntr_weekly_telecast_data')}}  
where (src_series_ilike_1 =1) and 
broadcast_date between '2017-01-01' and current_date 
)

select broadcast_date,date_trunc('week',broadcast_date + interval '24 hours') - interval '24 hours' as week_0,
src_channel, src_country,src_series,start_time_avg_tm,end_time_avg_tm,length_avg_tm,demographic,
rat_avg_wg_pct,rat_num_avg_wg,shr_avg_wg_pct,(length_avg_tm/60)*rat_num_avg_wg as "VH",ETL_INSERT_REC_DTTM,
'IMPACTWRESTLING' as property
from intm_weekly_international_tagging_impactwrestling