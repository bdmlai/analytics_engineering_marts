/*
*************************************************************************************************************************************************
   TableName   : intm_cpg_daily_prod_plcmnt_split_stage2
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : Splitparting the sub part by ","
   Version      Date            Author               Request
   1.0          03/04/2021      schakraborty            ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','merch_flag'],
                    post_hook = "grant select on {{ this }} to DA_SNAYAK_USER_ROLE"
 ) }}
with source_data as
(
select * from(select 
airdate,
logentryguid,
title,
typeofshow,
segmenttype,
com as comments,
merch_flag as flag
from (
SELECT airdate,
title,
typeofshow,
segmenttype,
logentryguid,
SPLIT_PART(comments,',',N) as com,
case when SPLIT_PART(lower(comments),',',N) like '%wearing merch%' then 1 else 0 end as merch_flag
FROM {{ref('base_cpg_daily_prod_plcmnt_split_stage1')}}
CROSS JOIN
(SELECT cast(N as INTEGER) as N FROM
(SELECT ROW_NUMBER() OVER (ORDER BY TRUE) AS N FROM {{ref('base_cpg_daily_prod_plcmnt_split_stage1')}}))
WHERE SPLIT_PART(comments,',',N) IS NOT NULL AND SPLIT_PART(comments,',',N) != ''
)where airdate >= '2020-01-01' order by airdate,flag desc)where flag=1
)
Select * from source_data