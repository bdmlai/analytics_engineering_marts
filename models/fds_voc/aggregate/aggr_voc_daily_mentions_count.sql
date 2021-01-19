{{
  config({
		'schema': 'fds_voc',"materialized": 'incremental','tags': "Phase 8","persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select substring(postedtimeest, 1, 10)::date as posted_date, "tag" as mentions, count(distinct id) as count_tweets,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_8' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from {{source('hive_udl_voc','tweet_tags')}}
{% if is_incremental() %}
	where date_trunc('mon', substring(postedtimeest, 1, 10)::date) = date_trunc('mon', add_months(current_date, -1))
{% endif %}
group by 1, 2