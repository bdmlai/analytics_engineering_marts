/*
*************************************************************************************************************************************************
   TableName   : base_cpg_daily_prod_plcmnt_split_stage1
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : Spiltparting the comments by "." and adding new rows for each sub part.
                Creating a merch flag for comments containing "Wearing Merch" 
   Version      Date            Author               Request
   1.0          03/04/2021      schakraborty           ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','merch_flag'],
                    post_hook = "grant select on {{ this }} to DA_SCHAKRABORTY_USER_ROLE"
 ) }}
with source_data as
(
select * from (select 
airdate,
logentryguid,
title,
typeofshow,
segmenttype,
com as comments,
merch_flag as flag
from (
SELECT airdate,
logentryguid,
logentrydbid,
SPLIT_PART(comment,'.',N) as com,
move,
matchtitle,
title,
segmenttype,
inpoint,
episodenumber,
typeofshow,
case when SPLIT_PART(lower(comment),'.',N) like '%wearing merch%' then 1 else 0 end as merch_flag
FROM {{source('pp_udl_nplus','raw_lite_log')}}
CROSS JOIN
(SELECT cast(N as integer) as N FROM
(SELECT ROW_NUMBER() OVER (ORDER BY TRUE) AS N 
FROM {{source('pp_udl_nplus','raw_lite_log')}}
))
WHERE SPLIT_PART(comment,'.',N) IS NOT NULL AND SPLIT_PART(comment,'.',N) != ''
)where airdate >= '2020-01-01' order by airdate,inpoint,flag desc)where flag=1
)
Select * from source_data