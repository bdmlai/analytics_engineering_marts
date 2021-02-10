{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, avg(ue) over(partition by src_series_name order by interval_start_date_id asc rows 2 preceding) as ue_3m_avg_nat,
avg(imp) over(partition by src_series_name order by interval_start_date_id asc rows 2 preceding) as imp_3m_avg_nat 
from
(select interval_start_date_id, src_series_name, (100.0000*sum(imp)/nullif(sum(ue), 0)) as avg_audience_pct, 
sum(ue) as ue, sum(imp) as imp 
from {{ref('intm_le_local_viewership')}}
group by 1,2)