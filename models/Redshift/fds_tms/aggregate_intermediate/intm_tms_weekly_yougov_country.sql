{{
  config({
		"materialized": 'ephemeral'
        })
}}

select date
	,title
	,avg_appetite_score
	,stddev_appetite_score
	,country_code
	
from
(
select start_date as date,title,avg_appetite_score_us as avg_appetite_score,
stddev_appetite_score_us as stddev_appetite_score,'US' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_uk as avg_appetite_score,
stddev_appetite_score_uk as stddev_appetite_score,'GB' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_germany as avg_appetite_score,
stddev_appetite_score_germany as stddev_appetite_score,'DE' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_france as avg_appetite_score,
stddev_appetite_score_france as stddev_appetite_score,'FR' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_italy as avg_appetite_score,
stddev_appetite_score_italy as stddev_appetite_score,'IT' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_australia as avg_appetite_score,
stddev_appetite_score_australia as stddev_appetite_score,'AU' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_brazil as avg_appetite_score,
stddev_appetite_score_brazil as stddev_appetite_score,'BR' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_mexico as avg_appetite_score,
stddev_appetite_score_mexico as stddev_appetite_score,'MX' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_japan as avg_appetite_score,
stddev_appetite_score_japan as stddev_appetite_score,'JP' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_russia as avg_appetite_score,
stddev_appetite_score_russia as stddev_appetite_score,'RU' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_spain as avg_appetite_score,
stddev_appetite_score_spain as stddev_appetite_score,'ES' as country_code from 
{{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_canada as avg_appetite_score,
stddev_appetite_score_canada as stddev_appetite_score,'CA' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_argentina as avg_appetite_score,
stddev_appetite_score_argentina as stddev_appetite_score,'AR' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_colombia as avg_appetite_score,
stddev_appetite_score_colombia as stddev_appetite_score,'CO' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
union all
select start_date as date,title,avg_appetite_score_s_korea as avg_appetite_score,
stddev_appetite_score_s_korea as stddev_appetite_score,'KR' as country_code from {{source('udl_pii','restricted_yougov_weekly_signal_data')}} 
where sub_categories='WWE Talent' 
)