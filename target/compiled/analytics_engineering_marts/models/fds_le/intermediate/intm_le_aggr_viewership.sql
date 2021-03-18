
with __dbt__CTE__intm_le_local_viewership as (

select interval_start_date_id, src_series_name, geography, 
avg(rtg_percent) as rtg_percent, sum(ue::decimal(20,4)) as ue, sum(imp::decimal(20,4)) as imp 
from "prod_entdwdb"."fds_nl"."fact_nl_monthly_local_market"
where src_playback_period_desc = 'Live+Same Day' and src_series_name in ('WWE SMACKDOWN','WWE ENTERTAINMENT') 
and interval_start_date_id >= 20190101 and src_demographic_group='TV Households P2+'
group by 1,2,3
)select *, avg(ue) over(partition by src_series_name order by interval_start_date_id asc rows 2 preceding) as ue_3m_avg_nat,
avg(imp) over(partition by src_series_name order by interval_start_date_id asc rows 2 preceding) as imp_3m_avg_nat 
from
(select interval_start_date_id, src_series_name, (100.0000*sum(imp)/nullif(sum(ue), 0)) as avg_audience_pct, 
sum(ue) as ue, sum(imp) as imp 
from __dbt__CTE__intm_le_local_viewership
group by 1,2)