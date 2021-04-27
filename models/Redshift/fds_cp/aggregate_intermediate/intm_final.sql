{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select granularity, platform, type, metric, a.year, a.month, week, 
case when granularity = 'MTD' then b.start_date 
     when granularity = 'YTD' then c.start_date else a.start_date end as start_date,
end_date, value, prev_year, prev_year_week, 
case when granularity = 'MTD' then b.prev_year_start_date 
     when granularity = 'YTD' then c.prev_year_start_date else a.prev_year_start_date end as prev_year_start_date,     
prev_year_end_date, prev_year_value
from {{ref('intm_consolidation')}} a
left join
(select year,month, min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from {{ref('intm_consolidation')}} group by 1,2) b
on a.year = b.year
and a.month = b.month
left join
(select year,min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from {{ref('intm_consolidation')}} group by 1 ) c
on a.year = c.year