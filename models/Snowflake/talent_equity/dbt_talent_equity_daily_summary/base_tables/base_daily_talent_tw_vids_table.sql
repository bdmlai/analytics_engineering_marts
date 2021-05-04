/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_tw_vids_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture #tw vids,views and engagements for talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','Twitter','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select full_date as date,character_lineage as lineage_name,
count(distinct dim_video_id) as tot_tw_vids,sum(public_engagements) as tot_eng_tw_vids,sum(minutes_viewed) as tot_mins_tw_vids,
sum(video_views) as tot_views_tw_vids
from {{source('sf_fds_tw','fact_tw_engagement_post')}} a
left join {{source('sf_cdm','dim_smprovider_account')}} b on a.dim_smprovider_account_id=b.dim_smprovider_account_id
left join {{source('sf_udl_cp','da_monthly_conviva_emm_accounts_mapping')}} c on b.account_name=c.all_conviva_accounts
left join {{source('sf_cdm','dim_date')}} e on a.dim_date_id=e.dim_date_id where dim_video_id is not null
group by 1,2


)
select * from 
    source_data