
select substring(postedtimeest, 1, 10)::date as posted_date, "tag" as mentions, count(distinct id) as count_tweets,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_8' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from "entdwdb"."hive_udl_voc"."tweet_tags"

group by 1, 2