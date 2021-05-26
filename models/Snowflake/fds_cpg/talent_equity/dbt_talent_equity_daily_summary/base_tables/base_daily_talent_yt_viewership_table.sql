/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_yt_viewership_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture #yt vids,views and engagements for talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
PSTA-3183   May 03 -2021 Enhancement for Facebook, YouTube & Twitter data in Scorecard DB
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','youtube','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}
with source_data as
(select k.*,talent from (
select wed.video_id, to_date(wed.time_uploaded) as upload_date, wed.report_date_dt as view_date,
sum(views) as views, sum(wed.watch_time_minutes) as minutes_watched, sum(likes+comments+coalesce(shares,0)) as engagements
from {{source('sf_fds_yt','rpt_yt_wwe_engagement_daily')}} wed where upload_date>='2018-01-01'  --PSTA-3183
and report_date_dt between upload_date and upload_date+30 group by 1,2,3
) k
left join {{source('sf_fds_yt','yt_amg_content_groups')}} acg on (k.video_id = acg.yt_id))
select * from 
    source_data
	
