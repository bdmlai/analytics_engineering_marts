
select interval_start_date_id, src_series_name, geography, 
avg(rtg_percent) as rtg_percent, sum(ue::decimal(20,4)) as ue, sum(imp::decimal(20,4)) as imp 
from "prod_entdwdb"."fds_nl"."fact_nl_monthly_local_market"
where src_playback_period_desc = 'Live+Same Day' and src_series_name in ('WWE SMACKDOWN','WWE ENTERTAINMENT') 
and interval_start_date_id >= 20190101 and src_demographic_group='TV Households P2+'
group by 1,2,3