/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_heelface_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture designation of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent equity','emm','heelface','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

SELECT b.character_lineages_wweid, b.character_lineage_name, a.designation, a.start_date, 
CASE WHEN a.end_date IS NULL THEN CURRENT_DATE ELSE a.end_date END AS end_date 
FROM {{source('sf_fds_emm','babyface_heel')}} a LEFT JOIN {{source('sf_fds_mdm','character')}} b 
ON a.character_wweid=b.characters_wweid GROUP BY 1,2,3,4,5
)
select * from 
    source_data
