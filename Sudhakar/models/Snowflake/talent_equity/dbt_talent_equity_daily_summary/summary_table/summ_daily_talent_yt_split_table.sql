/*
*************************************************************************************************************************************************
   TableName   : summ_daily_talent_yt_split_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Intermediate Summary Model to capture yt viewership of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent_equity','daily',
                        'youtube','centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select character_lineage_name as lineage_name,character_lineages_wweid as lineage_wweid,upload_date,
sum(tot_yt_vids) as tot_yt_vids,sum(tot_views) as tot_views,sum(tot_min_wat) as tot_min_wat,sum(tot_eng) as tot_eng
from(
select trim(name) as talent,upload_date,count(distinct a.video_id) as tot_yt_vids,
sum(views) as tot_views,sum(minutes_watched) as tot_min_wat,sum(engagements) as tot_eng 
from ({{ref('base_daily_talent_yt_viewership_table')}}) a 
left join ({{ref('intm_daily_talent_yt_split_table')}}) b on a.video_id=b.video_id group by 1,2)a
left join ({{source('sf_fds_mdm','character')}}) b on lower(a.talent)=lower(b.character_lineage_name)
group by 1,2,3
)
select * from 
    source_data
