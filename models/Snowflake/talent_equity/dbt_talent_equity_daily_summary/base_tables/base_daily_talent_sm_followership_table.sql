/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_sm_followership_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture social media followership for talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','Facebook','Instagram','Twitter','follower','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select full_date as date,character_lineage as lineage_name,
facebook_followers,instagram_followers,twitter_followers 
from {{source('sf_fds_cp','fact_co_smfollowership_cumulative_summary')}} a
left join {{source('sf_cdm','dim_smprovider_account')}} b on a.dim_smprovider_account_id=b.dim_smprovider_account_id
left join {{source('sf_udl_cp','da_monthly_conviva_emm_accounts_mapping')}} c on b.account_name=c.all_conviva_accounts
left join {{source('sf_cdm','dim_date')}} e on a.dim_date_id=e.dim_date_id 
where character_lineage<>'' group by 1,2,3,4,5


)
select * from 
    source_data