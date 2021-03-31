{{
  config({
		"schema": 'fds_tms',
		"materialized": 'table','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
	        'post-hook': 'grant select on {{ this }} to public'
                
  })
}}

select date
	,title
	,avg_appetite_score
	,stddev_appetite_score
	,country,
	'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id
	,'bi_dbt_user_prd' as etl_insert_user_id
	, current_timestamp as etl_insert_rec_dttm
	, null as etl_update_user_id
	, cast(null as timestamp) as etl_update_rec_dttm
from
(
select start_date as date,title,avg_appetite_score_us as avg_appetite_score,
stddev_appetite_score_us as stddev_appetite_score,'US' as country 
from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_uk as avg_appetite_score,
stddev_appetite_score_uk as stddev_appetite_score,'UK' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_germany as avg_appetite_score,
stddev_appetite_score_germany as stddev_appetite_score,'Germany' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_france as avg_appetite_score,
stddev_appetite_score_france as stddev_appetite_score,'France' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_italy as avg_appetite_score,
stddev_appetite_score_italy as stddev_appetite_score,'Italy' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_australia as avg_appetite_score,
stddev_appetite_score_australia as stddev_appetite_score,'Australia' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_brazil as avg_appetite_score,
stddev_appetite_score_brazil as stddev_appetite_score,'Brazil' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_mexico as avg_appetite_score,
stddev_appetite_score_mexico as stddev_appetite_score,'Mexico' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_japan as avg_appetite_score,
stddev_appetite_score_japan as stddev_appetite_score,'Japan' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_russia as avg_appetite_score,
stddev_appetite_score_russia as stddev_appetite_score,'Russia' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_spain as avg_appetite_score,
stddev_appetite_score_spain as stddev_appetite_score,'Spain' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_canada as avg_appetite_score,
stddev_appetite_score_canada as stddev_appetite_score,'Canada' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_argentina as avg_appetite_score,
stddev_appetite_score_argentina as stddev_appetite_score,'Argentina' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_colombia as avg_appetite_score,
stddev_appetite_score_colombia as stddev_appetite_score,'Colombia' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_s_korea as avg_appetite_score,
stddev_appetite_score_s_korea as stddev_appetite_score,'S_Korea' as country from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
)