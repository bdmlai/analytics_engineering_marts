/*
*************************************************************************************************************************************************
   TableName   : intm_daily_talent_yt_split_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Intermediate Model to capture yt viewership of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','daily',
                        'youtube','centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select video_id,value as name from ({{ref('base_daily_talent_yt_viewership_table')}}),
 lateral split_to_table(talent, ',')
group by 1,2
)
select * from 
    source_data
