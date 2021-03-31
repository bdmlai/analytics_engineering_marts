{{
  config({
		"materialized": 'ephemeral'
  })
}}

select broadcast_fin_week_begin_date as week,
broadcast_fin_week_num as week_number,
broadcast_fin_year as year,
case when src_program_id = '296881'  then 'RAW'
     when src_program_id = '898521' or src_program_id = '1000131' or src_program_id = '339681' then 'SmackDown'
     when src_program_id = '436999' then 'NXT' end as program_type,
src_demographic_group,
src_playback_period_cd,
avg(avg_audience_proj_000) as avg_audience_proj_000,
avg(avg_audience_pct) as avg_audience_pct,
count(*) as count_record
from  {{ref('rpt_nl_daily_wwe_program_ratings')}}
where src_program_id in ( '436999', '296881', '898521', '1000131', '339681') 
 and  src_program_attributes not like '%(R)%'
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6