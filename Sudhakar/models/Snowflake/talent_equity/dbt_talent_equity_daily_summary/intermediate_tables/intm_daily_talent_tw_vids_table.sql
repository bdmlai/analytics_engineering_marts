
/*
*************************************************************************************************************************************************
   TableName   : intm_daily_talent_tw_vids_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Intermediate Model to capture #tw vids,views and engagements for talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent equity','Twitter','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

SELECT a.date, a.lineage_name, a.lineage_wweid, a.gender, a.entity_type,
sum(tot_tw_vids) as tot_tw_vids,sum(tot_eng_tw_vids) as tot_eng_tw_vids,sum(tot_mins_tw_vids) as tot_mins_tw_vids
,sum(tot_views_tw_vids) as tot_views_tw_vids
FROM ({{ref('base_daily_talent_table')}}) a 
LEFT JOIN 
(
({{ref('base_daily_talent_tw_vids_table')}}))n 
ON a.lineage_name=n.lineage_name AND a.date= n.date 
GROUP BY 1,2,3,4,5


)
select * from 
    source_data