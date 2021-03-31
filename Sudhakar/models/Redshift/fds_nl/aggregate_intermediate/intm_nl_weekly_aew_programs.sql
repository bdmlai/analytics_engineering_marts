{{
  config({
		"materialized": 'ephemeral'
  })
}}

select e.fin_year_week_begin_date as week,
e.financial_year_week_number as week_number,
e.financial_year as year,
'AEW'  as program_type,
src_demographic_group,
src_playback_period_cd,
avg(avg_audience_proj_000) as avg_audience_proj_000,
avg(avg_audience_pct) as avg_audience_pct,
count(*) as record_count
from {{source('fds_nl','fact_nl_program_viewership_ratings')}} a 
left join {{source('cdm','dim_date')}} d on a.broadcast_date_id = d.dim_date_id
left join 
(select h.dim_date_id, trunc(financial_year_week_begin_date) as fin_year_week_begin_date, 
trunc(financial_year_week_end_date) as fin_year_week_end_date, financial_year_week_number, financial_month_number, 
mth_abbr_nm as financial_month_name, financial_quarter, financial_year
from {{source('udl_nl','nielsen_finance_yearly_calendar')}} h
join (select distinct cal_mth_num, mth_abbr_nm from {{source('cdm','dim_date')}}) i on h.financial_month_number = i.cal_mth_num
where dim_date_id >= 20140101) e on a.broadcast_date_id = e.dim_date_id
where src_program_id in ('459734') 
and src_program_attributes not like '%(R)%'
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
