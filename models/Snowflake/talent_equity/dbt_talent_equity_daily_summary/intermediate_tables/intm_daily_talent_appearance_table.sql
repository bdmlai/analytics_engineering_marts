/*
*************************************************************************************************************************************************
   TableName   : intm_daily_talent_appearance_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Intermediate Model to capture screentime and appearance of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','screentime','show','daily','centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

SELECT a.date, a.lineage_name, a.lineage_wweid, a.gender, a.entity_type, 
typeofshow,title,segmenttype,matchtype,role,sum(tot_dur) as duration
FROM ({{ref('base_daily_talent_table')}}) a 
LEFT JOIN (({{ref('base_daily_talent_appearance_table')}})) n 
ON a.lineage_name=n.lineage_name AND a.date= n.airdate 
GROUP BY 1,2,3,4,5,6,7,8,9,10 ORDER BY 2,1
)
select * from 
    source_data 