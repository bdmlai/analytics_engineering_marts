/*
*************************************************************************************************************************************************
   TableName   : summ_daily_talent_yt_viewership_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Summary Model to capture YouTube viewership of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent equity','youtube','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_SCHAKRABORTY_USER_ROLE"
 ) }}

with source_data as

(

select a.lineage_name,a.lineage_wweid,a.gender, a.entity_type,a.date,
sum(tot_yt_vids) as tot_yt_vids,sum(tot_views) as tot_views,sum(tot_min_wat) as tot_min_wat,sum(tot_eng) as tot_eng
FROM ({{ref('base_daily_talent_table')}}) a 
LEFT JOIN 
(
({{ref('summ_daily_talent_yt_split_table')}}))n 
ON a.lineage_name=n.lineage_name AND a.date= n.upload_date 
GROUP BY 1,2,3,4,5


)
select * from 
    source_data 