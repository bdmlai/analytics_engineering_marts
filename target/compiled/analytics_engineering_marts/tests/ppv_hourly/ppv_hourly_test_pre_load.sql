

select distinct query1 from
(select case when total>0 then null else 1 end query1 from
(select 'comp_ppvs' as metric_nm, 'n/a' as dm_dimension_val, count(event_name) as total from 
udl_nplus.raw_da_weekly_ppv_hourly_comps 
where event_date >= current_date and 
(comp1_event_name != '' or comp2_event_name != '' or comp3_event_name != ''))
union all
select case when total>0 then null else 1 end query2 from
(select 'total_adds_prev_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from 
(select date as adds_date, hour as adds_time,sum(paid_adds) as 
paid_adds,sum(trial_adds) as trial_adds, sum(paid_adds+trial_adds) as total_adds from 
udl_nplus.drvd_intra_hour_quarter_hour_adds where 
date = date(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate()))) and hour = extract(hour from 
dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate()))) group by 1,2 ) where total_adds != 0)
union all
select case when total>0 then null else 1 end query3 from
(select 'total_adds_prev_quarter_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from (select date as adds_date, hour as adds_time, 
quarter_hour,sum(paid_adds) as paid_adds, sum(trial_adds) as trial_adds, sum(paid_adds+trial_adds) as total_adds from 
udl_nplus.drvd_intra_hour_quarter_hour_adds where date = date(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate()))) and 
adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate()))) group by 1,2,3) where total_adds!= 0))