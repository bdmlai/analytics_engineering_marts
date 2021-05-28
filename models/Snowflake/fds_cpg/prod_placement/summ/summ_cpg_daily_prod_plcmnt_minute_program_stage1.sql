/*
*************************************************************************************************************************************************
   TableName   : cpg_daily_prod_plcmnt_minute_program_stage1
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : Calculating the program start time from minimum of inpoint.
   Version      Date            Author               Request
   1.0          03/04/2021      schakraborty            ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','logentryguid','inpoint'],
                    post_hook = "grant select on {{ this }} to DA_SNAYAK_USER_ROLE"
 ) }}
with source_data as
(
select a.airdate,
a.logentryguid,
a.title as show,
a.typeofshow,
a.segmenttype,
c.pgm_st_time,
b.inpoint,
cast(((substring(inpoint,1,2))*1)+12 as integer) as hour,
cast((substring(inpoint,4,2))*1 as integer) as mins
from {{ref('intm_cpg_daily_prod_plcmnt_split_stage4_update')}} a
left join {{source('pp_udl_nplus','raw_lite_log')}} b on a.logentryguid=b.logentryguid
left join (select airdate,
title,
showdbid,
min(inpoint) as pgm_st_time from {{source('pp_udl_nplus','raw_lite_log')}} 
group by 1,2,3)c 
on a.airdate=c.airdate and a.title=c.title 
group by 1,2,3,4,5,6,7,8,9 order by 1,2
)
Select * from source_data