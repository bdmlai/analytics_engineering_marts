
/*
*************************************************************************************************************************************************
   TableName   : intm_daily_talent_ig_post_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Intermediate Model to capture #ig posts and engagements for talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','Instagram','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

SELECT a.date, a.lineage_name, a.lineage_wweid, a.gender, a.entity_type,
sum(tot_ig_post) as tot_ig_post,sum(tot_eng_ig_post) as tot_eng_ig_post
FROM ({{ref('base_daily_talent_table')}}) a 
LEFT JOIN 
(({{ref('base_daily_talent_ig_post_table')}}))n 
ON a.lineage_name=n.lineage_name AND a.date= n.date 
GROUP BY 1,2,3,4,5


)
select * from 
    source_data