/*
*************************************************************************************************************************************************
   TableName   : rpt_weekly_domestic_tagging
   Schema	   : fds_nl
   Contributor : B.V.Sai Praveen Chakravarthy
   Description : summary table for capturing the tagged attributes of properties 
                 by performing union of all the intermediate ephemeral tables
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/


{{ config(materialized='incremental',incremental_strategy='delete+insert',
enabled = true,tags=['domestic','tagging','summary'],
unique_key= "src_broadcast_orig_date||'-'||src_broadcast_network_id||'-'||program_telecast_rpt_starttime
||'-'||src_broadcast_network_service_type",
schema ='fds_nl',post_hook = "grant select on {{ this }} to DA_RBAVISETTY_USER_ROLE") }}

with rpt_nl_weekly_domestic_tagging as (
select * from {{ref('intm_weekly_domestic_tagging_aew')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_mlb')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_nascar')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_nba')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_nfl')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_nhl')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_epl')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_bundesliga')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_mls')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_ncaa')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_open_championship')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_pfl')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_pga')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_premier_boxing')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_rydercup')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_top_rank_boxing')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_uefa')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_ufc')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_uot')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_usopengolf')}}
union all
select * from {{ref('intm_weekly_domestic_tagging_wimbledon')}}
) 

select * from
(
select *,
src_broadcast_orig_date||'-'||src_broadcast_network_id||'-'||program_telecast_rpt_starttime
||'-'||src_broadcast_network_service_type as id,
ROW_NUMBER() over (partition by src_broadcast_orig_date, src_broadcast_network_id,
program_telecast_rpt_starttime,src_broadcast_network_service_type order by inserted_time desc) ROWNUMBER 
from rpt_nl_weekly_domestic_tagging
)
where ROWNUMBER =1
{% if is_incremental() %} 
having inserted_time > (select max(inserted_time) from {{ this }}) 
{% endif %}
