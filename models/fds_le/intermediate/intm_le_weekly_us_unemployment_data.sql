{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, row_number() over (partition by state order by filed_week_ended desc) as rank
from
(select * from 
(select *, extract(week from filed_week_ended::date) as week_num, extract(year from filed_week_ended::date) as year_num,
row_number() over (partition by state, filed_week_ended order by as_on_date desc) as rank_1  
from {{source('sf_udl_cp','le_weekly_us_unemployment_claims_details')}})
where rank_1 = 1)