/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_twitter_mentions_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture Twitter Mentions for talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','twitter','mentions','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select posted_date as date,mentions as lineage_name,count_tweets from(
select posted_date,upper(talent_tag) as mentions,sum(count_tweets)as count_tweets 
from {{source('sf_fds_voc','aggr_voc_daily_mentions_count')}} a
left join {{source('sf_udl_tw','twitter_static_mentions_talent_tag')}} b on lower(a.mentions)=lower(b.talent_tag)
where posted_date>='2018-01-01' and posted_date< '2020-10-01' and a.mentions is not null group by 1,2 
union all
select posted_date,upper(character_lineage_name) as mentions,sum(count_tweets)as count_tweets 
from {{source('sf_fds_voc','aggr_voc_daily_mentions_count')}} a
left join {{source('sf_fds_mdm','character')}} b on lower(a.mentions)=lower(b.characters_name)
where posted_date>='2020-10-01' and mentions is not null group by 1,2)group by 1,2,3 

)
select * from 
    source_data