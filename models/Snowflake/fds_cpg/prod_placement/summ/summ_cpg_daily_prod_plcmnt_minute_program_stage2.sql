/*
*************************************************************************************************************************************************
   TableName   : summ_cpg_daily_prod_plcmnt_minute_program_stage2
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : Joining the min_pgm model for 21 times for creating 20 mins window.
   Version      Date            Author               Request
   1.0          03/04/2021      schakraborty          ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','min_pgm'],
                    post_hook = "grant select on {{ this }} to DA_SNAYAK_USER_ROLE"
 ) }}
with source_data as
(
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
union all
select * from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage1')}}
order by 1
)
Select * from source_data