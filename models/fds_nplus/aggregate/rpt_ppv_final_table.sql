 {{
  config({
	"schema": 'dt_prod_support',	
	"materialized": 'incremental',
	"pre-hook":["delete from dt_prod_support.rpt_ppv_final_table",
	"drop table if exists #current_ppv;
create table #current_ppv as 
(select as_on_date, event_name,  event_date, event_timestamp as event_dttm,
 event_reporting_type, event_type, update_date 
from udl_nplus.raw_da_weekly_ppv_hourly_comps_new
where update_date=(select max(update_date) from udl_nplus.raw_da_weekly_ppv_hourly_comps_new)
and as_on_date=(select max(as_on_date) from udl_nplus.raw_da_weekly_ppv_hourly_comps_new)
and exists (select 1 from udl_nplus.raw_da_weekly_ppv_hourly_comps_new where event_date between
trunc(convert_timezone('AMERICA/NEW_YORK', sysdate-1)) and trunc(convert_timezone('AMERICA/NEW_YORK', sysdate+7))
and event_type='current_ppv'));

-- Adding all week days to all the comps
drop table if exists #full_list;
create table #full_list as 
(select event_date, event_dttm, event_name, event_type, event_reporting_type, 0 as adds_days_to_event, event_date as adds_date from #current_ppv
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -1 as adds_days_to_event, event_date-1 as adds_date from #current_ppv
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -2 as adds_days_to_event, event_date-2 as adds_date from #current_ppv
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -3 as adds_days_to_event, event_date-3 as adds_date from #current_ppv
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -4 as adds_days_to_event, event_date-4 as adds_date from #current_ppv
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -5 as adds_days_to_event, event_date-5 as adds_date from #current_ppv
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -6 as adds_days_to_event, event_date-6 as adds_date from #current_ppv);


-- Adding all week days to all the comps
drop table if exists #hist_orders;
create table #hist_orders as 
select adds_date,adds_time,trial_adds,paid_adds,trial_adds+Paid_adds total_adds from (SELECT  ordr_dt adds_date, 
        hour_flag adds_time,
        COALESCE(Paid_add,0) as Paid_adds,
        COALESCE(Trial_add,0) as Trial_adds
        
FROM
(
SELECT  ordr_dt, 
        hour_flag,
        sum(case when ADD_TYPE = 'Trial' then add_cnt end) as Trial_add,
        sum(case when ADD_TYPE = 'Paid' then add_cnt end) as Paid_add
 FROM
 (
        
 select trunc(initial_order_dttm) ordr_dt, 
        date_part(hour,initial_order_dttm) hour_flag,
        case when trial_start_dttm is not null then 'Trial' else 'Paid' end as add_type,
        count(distinct order_id) add_cnt
 from fds_nplus.fact_daily_subscription_order_status 
 where trunc(initial_order_dttm) in (select distinct adds_date from #full_list)
 and payment_method in ('cybersource','stripe','incomm','paypal') --,'roku_iap'
 group by 1,2,3
 )
  group by 1,2
  ));

-- Adding days to ppv and comps
drop table if exists #final_table;
create table #final_table as 
select * from 
(select a.event_date, a.event_dttm, a.event_name, a.event_type, a.event_reporting_type, a.adds_days_to_event,
case
        when date_part(dayofweek,a.adds_date) = 0 then 'Sunday'
        when date_part(dayofweek,a.adds_date) = 1 then 'Monday'
        when date_part(dayofweek,a.adds_date) = 2 then 'Tuesday'
        when date_part(dayofweek,a.adds_date) = 3 then 'Wednesday'
        when date_part(dayofweek,a.adds_date) = 4 then 'Thursday'
        when date_part(dayofweek,a.adds_date) = 5 then 'Friday'
        when date_part(dayofweek,a.adds_date) = 6 then 'Saturday'
else 'Other' end as adds_day_of_week,
a.adds_date, b.adds_time, b.paid_adds, b.trial_adds, b.total_adds
from #full_list as a
left join (
        select adds_date, adds_time, paid_adds, trial_adds, total_adds
        from #hist_orders
) as b on a.adds_date = b.adds_date);

--taking only current_ppv and comps Friday Saturday and Sunday
drop table if exists #final_table_up;
create table #final_table_up as
select * from #final_table where event_type = 'current_ppv'
union all
select * from #final_table where event_type != 'current_ppv' and adds_day_of_week in ('Friday','Saturday','Sunday');

--select *,convert_timezone('AMERICA/NEW_YORK', sysdate) as etl_insert_dtmm  from #final_table_up

-- comp3 removed for purpose and Monday excluded Payback 

-- calculating pct values for comps hourly percent excluding comp3
drop table if exists #t1_hourly_comps_pct;
create table #t1_hourly_comps_pct as
select  (ag_hour*1.00)/ag_sum ag_sum_pct,a.adds_day_of_week,adds_time from
(select ag_hour,adds_day_of_week,adds_time from
(select avg(paid_adds)+avg(trial_adds) ag_hour,adds_time,adds_day_of_week from #final_table_up where adds_day_of_week in ('Friday','Saturday','Sunday') 
and event_type !='comp3' group by adds_time,adds_day_of_week)) a 
left join
(select sum(ag_hour) ag_sum,adds_day_of_week from
(select avg(paid_adds)+avg(trial_adds) ag_hour,adds_day_of_week from #final_table_up where adds_day_of_week in ('Friday','Saturday','Sunday') 
and event_type !='comp3' group by adds_time,adds_day_of_week)
group by 2) b
on a.adds_day_of_week=b.adds_day_of_week;

-- calculating pct values for comps Go home week excluding comp3 


drop table if exists #t2_ghw_total;
create table #t2_ghw_total as
(select sum(ghw_avg) tot_sum from
(select avg(ghw_sum) ghw_avg,adds_day_of_week from
(select sum(paid_adds)+sum(trial_adds) ghw_sum ,adds_day_of_week,event_name  from #final_table where event_type not in ('current_ppv','comp3')
group by adds_day_of_week,event_name) group by 2
));


drop table if exists #t2_ghw_pct;
create table #t2_ghw_pct as
select (ghw_avg*1.00)/tot_sum  tot_pct,adds_day_of_week from
(select avg(ghw_sum) ghw_avg,adds_day_of_week from
(select sum(paid_adds)+sum(trial_adds) ghw_sum ,adds_day_of_week,event_name  from #final_table where event_type not in ('current_ppv','comp3')  
group by adds_day_of_week,event_name) group by 2) , #t2_ghw_total;


drop table if exists #forcst_pct_calc ;
create table #forcst_pct_calc as 
(select trunc(bill_date) as bill_date,
case
        when date_part(dayofweek,bill_date) = 0 then 'Sunday'
        when date_part(dayofweek,bill_date) = 1 then 'Monday'
        when date_part(dayofweek,bill_date) = 2 then 'Tuesday'
        when date_part(dayofweek,bill_date) = 3 then 'Wednesday'
        when date_part(dayofweek,bill_date) = 4 then 'Thursday'
        when date_part(dayofweek,bill_date) = 5 then 'Friday'
        when date_part(dayofweek,bill_date) = 6 then 'Saturday'
else 'Other' end as bill_day_of_week,
sum(paid_new_adds+paid_winbacks) as current_day_forecast
from fds_nplus.aggr_nplus_daily_forcast_output
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_daily_forcast_output)
and UPPER(payment_method) in ('MLBAM') and Upper(official_run_flag)='OFFICIAL' and
trunc(bill_date) between '2020-10-19' and '2020-10-25'
group by 1,2);

drop table if exists #forcast_pct_calc;
create table #forcast_pct_calc as 
select bill_day_of_week,current_day_forecast/tot_forecast final_forecast_pct from  #forcst_pct_calc,
(select sum(current_day_forecast) tot_forecast from  #forcst_pct_calc) a;

drop table if exists #forcst_ghw_pct_avg;
create table #forcst_ghw_pct_avg as 
select (tot_pct+final_forecast_pct)/2 tot_pct,adds_day_of_week from
(select adds_day_of_week,tot_pct from #t2_ghw_pct) a join
(select bill_day_of_week,final_forecast_pct from #forcast_pct_calc) b on a.adds_day_of_week=b.bill_day_of_week;

drop table if exists #t3_comps_ghw_hour_moving_pct;
create table #t3_comps_ghw_hour_moving_pct as 
select sum(ag_sum_pct) over (partition by adds_day_of_week order by adds_time,adds_day_of_week rows unbounded preceding) ag_moving_pct,adds_time,adds_day_of_week 
from #t1_hourly_comps_pct;

drop table if exists #t3_comps_ghw_hour_pct;
create table #t3_comps_ghw_hour_pct as 
select case 
	when adds_day_of_week='Friday' then
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week not in ('Friday','Saturday','Sunday'))+
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week  = 'Friday')*
			(select ag_moving_pct from #t3_comps_ghw_hour_moving_pct where adds_time=a.adds_time and adds_day_of_week  =a.adds_day_of_week)
	when adds_day_of_week='Saturday' then
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week not in ('Saturday','Sunday'))+
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week  = 'Saturday')*
			(select ag_moving_pct from #t3_comps_ghw_hour_moving_pct where adds_time=a.adds_time and adds_day_of_week  = a.adds_day_of_week)
	when adds_day_of_week='Sunday' then
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week not in ('Sunday'))+
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week  = 'Sunday')*
			(select ag_moving_pct from #t3_comps_ghw_hour_moving_pct where adds_time=a.adds_time and adds_day_of_week  = a.adds_day_of_week) end day_pct,
			adds_day_of_week,adds_time  from #t1_hourly_comps_pct a;

--Calculating percent calculation for estimate calculation 
/*drop table if exists #t3_comps_ghw_hour_pct;
create table #t3_comps_ghw_hour_pct as 
select case 
	when adds_day_of_week='Friday' then
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week not in ('Friday','Saturday','Sunday'))+
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week  = 'Friday')*
			(select sum(ag_sum_pct) from #t1_hourly_comps_pct where adds_time<a.adds_time and adds_day_of_week  =a.adds_day_of_week)
	when adds_day_of_week='Saturday' then
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week not in ('Saturday','Sunday'))+
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week  = 'Saturday')*
			(select sum(ag_sum_pct) from #t1_hourly_comps_pct where adds_time<a.adds_time and adds_day_of_week  = a.adds_day_of_week)
	when adds_day_of_week='Sunday' then
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week not in ('Sunday'))+
			(select sum(tot_pct) from #forcst_ghw_pct_avg where adds_day_of_week  = 'Sunday')*
			(select sum(ag_sum_pct) from #t1_hourly_comps_pct where adds_time<a.adds_time and adds_day_of_week  = a.adds_day_of_week) end day_pct,
			adds_day_of_week,adds_time  from #t1_hourly_comps_pct a;*/
			
delete from dt_prod_support.rpt_ppv_hourly_pct;

insert into dt_prod_support.rpt_ppv_hourly_pct(adds_day_of_week,adds_time,pct,d2_pct)
select a.adds_day_of_week,a.adds_time,a.ag_sum_pct,b.day_pct from #t1_hourly_comps_pct a join 
#t3_comps_ghw_hour_pct b on a.adds_day_of_week=b.adds_day_of_week and a.adds_time=b.adds_time;

insert into  dt_prod_support.rpt_ppv_hourly_pct(gwh_pct,gwh_pct_day)
select tot_pct,adds_day_of_week from #forcst_ghw_pct_avg;				
"]
		})
}}
select *,convert_timezone('AMERICA/NEW_YORK', sysdate) as etl_insert_dtmm  from #final_table_up