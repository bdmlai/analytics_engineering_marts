
with dbt__CTE__INTERNAL_test as (

select distinct * from(select case when total=0 then null else 1 end query2 from
(select 'forecasted_total_adds_fri_sat_sun' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from 
(select current_adds_days_to_event, sum(current_day_forecast) as current_day_forecast,sum(weekend_forecast) as weekend_forecast,
case when sum(current_day_forecast) is null or sum(current_day_forecast) = 0 or sum(weekend_forecast) = 0 or sum(weekend_forecast) is null then 
1 else 0 end as flag from fds_nplus.rpt_network_ppv_actuals_estimates_forecast  where event_type = 'current_ppv' and current_adds_days_to_event in 
(0,-1,-2) group by 1 ) where flag=1)
union all
select case when total=0 then null else 1 end query6 from
(select 'total_adds_comp1_prev_24_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from (select adds_time, count(*) from 
fds_nplus.rpt_network_ppv_actuals_estimates_forecast where event_type = 'comp1'and (total_adds is null or total_adds = 0) group by 1 having count(*) >= 1 ))
union all
select case when total=0 then null else 1 end query7 from
(select 'total_adds_comp2_prev_24_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from (select adds_time, count(*) from 
fds_nplus.rpt_network_ppv_actuals_estimates_forecast where event_type = 'comp2' and (total_adds is null or total_adds = 0) group by 1 having count(*) >= 1 ))
union all
select case when total=0 then null else 1 end query8 from
(select 'total_adds_comp3_prev_24_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from (select adds_time, count(*) 
from fds_nplus.rpt_network_ppv_actuals_estimates_forecast where event_type = 'comp3' and (total_adds is null or total_adds = 0) 
group by 1 having count(*) >= 1 ))
union all
select case when total=0 then null else 1 end query9 from
(select 'total_adds_curr_ppv_prev_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from (select adds_date,total_adds, 
case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and adds_date = current_date-1 and adds_time = 23.0 and 
(total_adds is null or total_adds = 0) then 1      when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and      
adds_date = current_date and adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))      
and (total_adds is null or total_adds = 0) then 1       else 0 end as flag from fds_nplus.rpt_network_ppv_actuals_estimates_forecast  
where event_type = 'current_ppv' and adds_date >= current_date-1 ) where flag=1)
union all
select case when total=0 then null else 1 end query10 from
(select 'total_adds_curr_ppv_vs_comp1_prev_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from (select case when 
(cur_ppv_zero_hr_tot_adds < 0.5 * comp1_zero_hr_tot_adds) or (cur_ppv_zero_hr_tot_adds > 2 * comp1_zero_hr_tot_adds) then 1      
when (cur_ppv_otr_hr_tot_adds < 0.5 * comp1_otr_hr_tot_adds) or (cur_ppv_otr_hr_tot_adds > 2 * comp1_otr_hr_tot_adds) then 1      
else 0 end as flag from ( select  case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and      
adds_date = current_date-1 and adds_time = 23.0 and event_type = 'current_ppv' then total_adds end as cur_ppv_zero_hr_tot_adds, 
case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and adds_date = current_date 
and adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))       
and event_type = 'current_ppv' then total_adds end as cur_ppv_otr_hr_tot_adds, case when 
extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and 
adds_date = current_date-1 and adds_time = 23.0 and event_type = 'comp1' then total_adds end as 
comp1_zero_hr_tot_adds, case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and 
adds_date = current_date and adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))       
and event_type = 'comp1' then total_adds end as comp1_otr_hr_tot_adds from fds_nplus.rpt_network_ppv_actuals_estimates_forecast  
where adds_date >= current_date-1 ) ) where flag=1)
union all
select case when total=0 then null else 1 end query11 from
(select 'total_adds_curr_ppv_vs_comp2_prev_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from ( select case when 
(cur_ppv_zero_hr_tot_adds < 0.5 * comp2_zero_hr_tot_adds) or (cur_ppv_zero_hr_tot_adds > 2 * comp2_zero_hr_tot_adds) then 1     
 when (cur_ppv_otr_hr_tot_adds < 0.5 * comp2_otr_hr_tot_adds) or (cur_ppv_otr_hr_tot_adds > 2 * comp2_otr_hr_tot_adds) then 1      
 else 0 end as flag from ( select  case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and    
 adds_date = current_date-1 and adds_time = 23.0 and event_type = 'current_ppv' then total_adds end as cur_ppv_zero_hr_tot_adds,
 case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and      
 adds_date = current_date and adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))   
 and event_type = 'current_ppv' then total_adds end as cur_ppv_otr_hr_tot_adds, case when 
 extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and    
 adds_date = current_date-1 and adds_time = 23.0 and event_type = 'comp2' then total_adds end as comp2_zero_hr_tot_adds, 
 case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and      
 adds_date = current_date and adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))       
 and event_type = 'comp2' then total_adds end as comp2_otr_hr_tot_adds from fds_nplus.rpt_network_ppv_actuals_estimates_forecast 
 where adds_date >= current_date-1 ) ) where flag=1)
union all
select case when total=0 then null else 1 end query12 from 
(select 'total_adds_curr_ppv_vs_comp3_prev_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from ( select case when 
(cur_ppv_zero_hr_tot_adds < 0.5 * comp3_zero_hr_tot_adds) or (cur_ppv_zero_hr_tot_adds > 2 * comp3_zero_hr_tot_adds) then 1      
when (cur_ppv_otr_hr_tot_adds < 0.5 * comp3_otr_hr_tot_adds) or (cur_ppv_otr_hr_tot_adds > 2 * comp3_otr_hr_tot_adds) then 1      
else 0 end as flag from ( select case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and     
 adds_date = current_date-1 and adds_time = 23.0 and event_type = 'current_ppv' then total_adds end as cur_ppv_zero_hr_tot_adds, case when 
 extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and      adds_date = current_date and 
 adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))       and event_type = 'current_ppv' then 
 total_adds end as cur_ppv_otr_hr_tot_adds, case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and      
 adds_date = current_date-1 and adds_time = 23.0 and event_type = 'comp3' then total_adds end as comp3_zero_hr_tot_adds, case when 
 extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and      adds_date = current_date and 
 adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))       and event_type = 'comp3' then 
 total_adds end as comp3_otr_hr_tot_adds from fds_nplus.rpt_network_ppv_actuals_estimates_forecast  where adds_date >= current_date-1 )) where flag=1)
 union all
select case when total=0 then null else 1 end query13 from  
(select 'total_adds_curr_day_adds_prev_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from (select adds_date,total_adds,
currentday_adds_tillnow,currentday_adds_estimate, case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and
      adds_date = current_date-1 and adds_time = 23.0 and      (currentday_adds_tillnow <= total_adds or currentday_adds_estimate <= total_adds) then 1 
	  when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and      adds_date = current_date and 
	  adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate()))) and      
	  (currentday_adds_tillnow <= total_adds or currentday_adds_estimate <= total_adds) then 1       else 0 end as flag from 
	  fds_nplus.rpt_network_ppv_actuals_estimates_forecast  where event_type = 'current_ppv' and adds_time = extract(hour 
	  from(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))) and adds_date >= current_date-1 ) where flag=1)
union all
select case when total=0 then null else 1 end query14 from 	  
(select 'wknd_adds_curr_day_adds_prev_hour' as metric_nm, 'n/a' as dm_dimension_val, count(*) as total from 
(select adds_date,weekend_adds_tillnow ,currentday_adds_tillnow,weekend_adds_estimate,currentday_adds_estimate, 
case when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) = 0 and      adds_date = current_date-1 and 
adds_time = 23.0 and      (weekend_adds_tillnow < currentday_adds_tillnow or weekend_adds_estimate < currentday_adds_estimate) then 1       
when extract(hour from dateadd('hour',0,convert_timezone('AMERICA/NEW_YORK', getdate()))) > 0 and      adds_date = current_date and 
adds_time = extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate()))) and      (weekend_adds_tillnow < currentday_adds_tillnow or 
weekend_adds_estimate < currentday_adds_estimate) then 1       else 0 end as flag from fds_nplus.rpt_network_ppv_actuals_estimates_forecast  where 
event_type = 'current_ppv' and adds_time = extract(hour from(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))) and 
adds_date >= current_date-1 ) where flag=1)) where query2>0
)select count(*) from dbt__CTE__INTERNAL_test