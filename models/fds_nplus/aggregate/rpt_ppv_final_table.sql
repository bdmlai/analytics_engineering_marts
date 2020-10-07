 {{
  config({
	"schema": 'dt_prod_support',	
	"materialized": 'incremental',
	"pre-hook":["drop table if exists #current_ppv;
create table #current_ppv as 
(select as_on_date, event_name,  event_date, event_timestamp as event_dttm,
 event_reporting_type, event_type, update_date 
from udl_nplus.raw_da_weekly_ppv_hourly_comps_new
where update_date=(select max(update_date) from udl_nplus.raw_da_weekly_ppv_hourly_comps_new)
and as_on_date=(select max(as_on_date) from udl_nplus.raw_da_weekly_ppv_hourly_comps_new)
and exists (select 1 from udl_nplus.raw_da_weekly_ppv_hourly_comps_new where event_date between
trunc(convert_timezone('AMERICA/NEW_YORK', sysdate-1)) and trunc(convert_timezone('AMERICA/NEW_YORK', sysdate+7))
and event_type='current_ppv'));

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
 and payment_method in ('cybersource','stripe','incomm','paypal','roku_iap')
 group by 1,2,3
 )
  group by 1,2
  ));

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

drop table if exists #final_table_up;
create table #final_table_up as
select * from #final_table where event_type = 'current_ppv'
union all
select * from #final_table where event_type != 'current_ppv' and adds_day_of_week in ('Friday','Saturday','Sunday');

--select *,convert_timezone('AMERICA/NEW_YORK', sysdate) as etl_insert_dtmm  from #final_table_up


drop table if exists #t1;
create table #t1 as
select  (ag_hour*1.00)/ag_sum ag_sum_pct,a.adds_day_of_week,adds_time from
(select ag_hour,adds_day_of_week,adds_time from
(select avg(paid_adds)+avg(trial_adds) ag_hour,adds_time,adds_day_of_week from #final_table_up where adds_day_of_week in ('Friday','Saturday','Sunday') group by adds_time,adds_day_of_week)) a 
left join
(select sum(ag_hour) ag_sum,adds_day_of_week from
(select avg(paid_adds)+avg(trial_adds) ag_hour,adds_day_of_week from #final_table_up where adds_day_of_week in ('Friday','Saturday','Sunday') group by adds_time,adds_day_of_week)
group by 2) b
on a.adds_day_of_week=b.adds_day_of_week;

drop table if exists #t2;
create table #t2 as
select (ghw_avg*1.00)/tot_sum  tot_pct,adds_day_of_week from
(select avg(ghw_sum) ghw_avg,adds_day_of_week from
(select sum(paid_adds)+sum(trial_adds) ghw_sum ,adds_day_of_week,event_name  from #final_table where event_type!='current_ppv' 
group by adds_day_of_week,event_name) group by 2),
(select sum(ghw_avg) tot_sum from
(select avg(ghw_sum) ghw_avg,adds_day_of_week from
(select sum(paid_adds)+sum(trial_adds) ghw_sum ,adds_day_of_week,event_name  from #final_table where event_type!='current_ppv' 
group by adds_day_of_week,event_name) group by 2)) b ;



drop table if exists #t3;
create table #t3 as 
select case 
	when adds_day_of_week='Friday' then
			(select sum(tot_pct) from #t2 where adds_day_of_week not in ('Friday','Saturday','Sunday'))+
			(select sum(tot_pct) from #t2 where adds_day_of_week  = 'Friday')*
			(select sum(ag_sum_pct) from #t1 where adds_time<a.adds_time and adds_day_of_week  =a.adds_day_of_week)
	when adds_day_of_week='Saturday' then
			(select sum(tot_pct) from #t2 where adds_day_of_week not in ('Saturday','Sunday'))+
			(select sum(tot_pct) from #t2 where adds_day_of_week  = 'Saturday')*
			(select sum(ag_sum_pct) from #t1 where adds_time<a.adds_time and adds_day_of_week  = a.adds_day_of_week)
	when adds_day_of_week='Sunday' then
			(select sum(tot_pct) from #t2 where adds_day_of_week not in ('Sunday'))+
			(select sum(tot_pct) from #t2 where adds_day_of_week  = 'Sunday')*
			(select sum(ag_sum_pct) from #t1 where adds_time<a.adds_time and adds_day_of_week  = a.adds_day_of_week) end day_pct,
			adds_day_of_week,adds_time  from #t1 a;
			

delete from dt_prod_support.rpt_ppv_hourly_pct;

insert into dt_prod_support.rpt_ppv_hourly_pct(adds_day_of_week,adds_time,pct,d2_pct)
select a.adds_day_of_week,a.adds_time,a.ag_sum_pct,b.day_pct from #t1 a join 
#t3 b on a.adds_day_of_week=b.adds_day_of_week and a.adds_time=b.adds_time;

insert into  dt_prod_support.rpt_ppv_hourly_pct(gwh_pct,gwh_pct_day)
select tot_pct,adds_day_of_week from #t2;
"]
		})
}}
select *,convert_timezone('AMERICA/NEW_YORK', sysdate) as etl_insert_dtmm  from #final_table_up