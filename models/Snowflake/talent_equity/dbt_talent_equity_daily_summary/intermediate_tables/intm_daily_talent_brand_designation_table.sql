/*
*************************************************************************************************************************************************
   TableName   : intm_daily_talent_brand_designation_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Intermediate Model to capture brand and designation and absence of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','emm','brand','daily',
                        'heelface','absence','centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

SELECT a.date, a.lineage_name, a.lineage_wweid, a.gender, a.entity_type, m.designation AS brand, n.designation AS heel_face, 
o.absence AS absence 
FROM ({{ref('base_daily_talent_table')}}) a 
LEFT JOIN ( ({{ref('base_daily_talent_brand_table')}})) m 
ON a.lineage_name=m.character_lineage_name AND m.start_date <= a.date AND a.date <= m.end_date 
LEFT JOIN ( ({{ref('base_daily_talent_heelface_table')}})) n 
ON a.lineage_name=n.character_lineage_name AND n.start_date <= a.date AND a.date <= n.end_date 
LEFT JOIN ( ({{ref('base_daily_talent_absence_table')}})) o 
ON a.lineage_name=o.character_lineage_name AND o.start_date <= a.date AND a.date <= o.end_date 
GROUP BY 1,2,3,4,5,6,7,8 ORDER BY 2,1

)
select * from 
    source_data
