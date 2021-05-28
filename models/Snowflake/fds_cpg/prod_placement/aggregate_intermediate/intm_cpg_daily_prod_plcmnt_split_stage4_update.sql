/*
*************************************************************************************************************************************************
   TableName   : intm_cpg_daily_prod_plcmnt_split_stage4_update
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : Updating the comments by replacing "wearing merch" with blank
   Version      Date            Author               Request
   1.0          03/04/2021      schakraborty         ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','comments'],
                    post_hook = "grant select on {{ this }} to DA_SNAYAK_USER_ROLE"
 ) }}
with source_data as
(
select airdate,
title,
typeofshow,
segmenttype,
logentryguid,comments as com,
trim(SUBSTRING (comments,0,CHARINDEX(' wearing merch',comments))) as comments,
flag from {{ref('intm_cpg_daily_prod_plcmnt_split_stage4')}}
group by 1,2,3,4,5,6,7,8
)
Select * from source_data