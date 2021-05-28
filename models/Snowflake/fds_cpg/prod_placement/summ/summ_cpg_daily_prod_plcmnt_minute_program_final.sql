/*
*************************************************************************************************************************************************
   TableName   : summ_cpg_daily_prod_plcmnt_minute_program_final
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : Ranking the rows for distinct logentryguid
   Version      Date            Author               Request
   1.0          03/04/2021      schakraborty            ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','inpoint'],
                    post_hook = "grant select on {{ this }} to DA_SNAYAK_USER_ROLE"
 ) }}
with source_data as
(
select *,
row_number() over 
(partition by logentryguid
 order by inpoint asc) as min_pgm_val from {{ref('summ_cpg_daily_prod_plcmnt_minute_program_stage2')}} order by 1,2,9
 )
 Select * from source_data