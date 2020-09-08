 {{
  config({
	"schema": 'fds_nplus',	
	"materialized": 'incremental',
	"pre-hook":"delete from fds_nplus.rpt_network_ppv_actuals_estimates_forecast"
		})
}}

select * from (with current_ppv as 
(select *
from {{source('udl_nplus','raw_da_weekly_ppv_hourly_comps')}} where event_date between date(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))-1 and  date(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))+7),

-- Creating a transposed table with comp events --
 current_list as 
(select event_reporting_type, event_date, event_timestamp as event_dttm, event_name, 'current_ppv' as event_type from current_ppv
union all
select event_reporting_type, comp1_event_date as event_date, comp1_event_timestamp as event_dttm, comp1_event_name as event_name, 'comp1' as event_type from current_ppv
union all
select event_reporting_type, comp2_event_date as event_date, comp2_event_timestamp as event_dttm, comp2_event_name as event_name, 'comp2' as event_type from current_ppv
union all
select event_reporting_type, comp3_event_date as event_date, comp3_event_timestamp as event_dttm, comp3_event_name as event_name, 'comp3' as event_type from current_ppv
),
-- Creating a table with dates for the full go home week for current PPV and comps --
full_list as 
(select event_date, event_dttm, event_name, event_type, event_reporting_type, 0 as adds_days_to_event, event_date as adds_date from current_list
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -1 as adds_days_to_event, event_date-1 as adds_date from current_list
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -2 as adds_days_to_event, event_date-2 as adds_date from current_list
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -3 as adds_days_to_event, event_date-3 as adds_date from current_list
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -4 as adds_days_to_event, event_date-4 as adds_date from current_list
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -5 as adds_days_to_event, event_date-5 as adds_date from current_list
union all
select event_date, event_dttm, event_name, event_type, event_reporting_type, -6 as adds_days_to_event, event_date-6 as adds_date from current_list
),
-- Fetching historical orders for relevant comp dates and current PPV using SOS. Code is setup to exclude Roku and any other new payment method which does not have a forecast --
hist_orders as 
(select trunc(initial_order_dttm) as adds_date, 
date_part('hour',initial_order_dttm) as adds_time, 
sum(case when first_charged_dttm is null then 1 else 0 end) as trial_adds,
sum(case when first_charged_dttm is not null then 1 else 0 end) as paid_adds,
count(*) as total_adds from {{source('fds_nplus','fact_daily_subscription_order_status')}}
where trunc(initial_order_dttm) + 1 = as_on_date
and  trunc(initial_order_dttm) in (select distinct adds_date from full_list)
and trunc(as_on_date) - 1 in (select distinct adds_date from full_list)
and payment_method in ('cybersource','stripe','incomm','paypal')
group by 1,2 order by 1,2),
final_table as 
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
from full_list as a
left join (
        select adds_date, adds_time, paid_adds, trial_adds, total_adds
        from hist_orders
) as b
on a.adds_date = b.adds_date),
-- This view uses the above view to generate estimate for the full week --
-- This view will drive the Tableau dashboard --
final_view as 
(select 
event_date, event_dttm, event_name, event_type, event_reporting_type, 
adds_days_to_event, adds_day_of_week, adds_date, adds_time - extract(hour from event_dttm) as adds_time_to_event,
adds_time, paid_adds, trial_adds, total_adds
from final_table where adds_time is not null

union all
        (select a.event_date, a.event_dttm, a.event_name, a.event_type, a.event_reporting_type, 
        a.adds_days_to_event, a.adds_day_of_week, a.adds_date, b.adds_time - extract(hour from a.event_dttm) as adds_time_to_event,
        b.adds_time, b.paid_adds, b.trial_adds, b.total_adds
        from final_table as a
        inner join (
                select date as adds_date, hour as adds_time, 
                sum(paid_adds) as paid_adds, sum(trial_adds) as trial_adds, sum(paid_adds+trial_adds) as total_adds
                from {{source('udl_nplus','drvd_intra_hour_quarter_hour_adds')}}  a
		--where date = date(convert_timezone('AMERICA/NEW_YORK', getdate()))
		--TAB-2028 
		where date = date(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))
		and adds_time <= extract(hour from dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))
	      group by 1,2
        ) as b
        on a.adds_date = b.adds_date
        where a.adds_time is null
)
),
estimates as 
(select event_reporting_type, 
event_name,
event_date,
event_dttm,
current_adds_date, 
current_adds_days_to_event,
current_adds_day_of_week,
current_adds_time,
ghw_adds_tillnow, 
case when ghw_adds_tillnow > 0 and comp_ghw_adds_tillnow > 0 then ghw_adds_tillnow/(comp_ghw_adds_tillnow/comp_ghw_adds) else -1 end as ghw_adds_estimate,
currentday_adds_tillnow,
case when currentday_adds_tillnow > 0 and comp_currentday_adds_tillnow > 0 then currentday_adds_tillnow/(comp_currentday_adds_tillnow/comp_currentday_adds) else -1 end as currentday_adds_estimate,
weekend_adds_tillnow,
case when weekend_adds_tillnow > 0 and comp_weekend_adds_tillnow > 0 then (ghw_adds_tillnow/(comp_ghw_adds_tillnow/comp_ghw_adds) - ghw_adds_tillnow + weekend_adds_tillnow) else -1 end as weekend_adds_estimate
from (
        select
        max(event_reporting_type) as event_reporting_type,
        max(case when event_type = 'current_ppv' then event_name else null end) as event_name,
        max(case when event_type = 'current_ppv' then event_date else null end) as event_date, 
        max(case when event_type = 'current_ppv' then event_dttm else null end) as event_dttm, 
        max(case when current_days_to_event = 1 and event_type = 'current_ppv' then adds_date else null end) as current_adds_date,
        max(case when current_days_to_event = 1 and event_type = 'current_ppv' then adds_days_to_event else null end) as current_adds_days_to_event,
        max(case when current_days_to_event = 1 and event_type = 'current_ppv' then adds_day_of_week else null end) as current_adds_day_of_week,
        max(case when current_days_to_event = 1 and current_time_to_event = 1 and event_type = 'current_ppv' then adds_time else null end) as current_adds_time,
        sum(case when event_type = 'current_ppv' and adds_days_to_event between -2 and 0 then total_adds::float else 0 end) as weekend_adds_tillnow,
        sum(case when event_type = 'current_ppv' and adds_days_to_event between -6 and 0 then total_adds::float else 0 end) as ghw_adds_tillnow,
        sum(case when current_days_to_event = 1  and event_type = 'current_ppv' then total_adds::float else 0 end) as currentday_adds_tillnow,
        sum(case when event_type != 'current_ppv' and adds_days_to_event between -6 and 0 then total_adds::float else 0 end) as comp_ghw_adds,
        sum(case when event_type != 'current_ppv' and adds_days_to_event between -2 and 0 then total_adds::float else 0 end) as comp_weekend_adds,
        sum(case when current_time_to_event = 1 and event_type != 'current_ppv' and adds_days_to_event between -6 and 0 then total_adds::float else 0 end) as comp_ghw_adds_tillnow,
        sum(case when current_time_to_event = 1 and event_type != 'current_ppv' and adds_days_to_event between -2 and 0 then total_adds::float else 0 end) as comp_weekend_adds_tillnow,
        sum(case when current_days_to_event = 1 and event_type != 'current_ppv' then total_adds::float else 0 end) as comp_currentday_adds,
        sum(case when current_days_to_event = 1 and current_time_to_event = 1 and event_type != 'current_ppv' then total_adds::float else 0 end) as comp_currentday_adds_tillnow
        from (
                select a.*, 
                case when b.adds_days_to_event is null then 0 else 1 end as current_days_to_event,
                case when c.adds_time_to_event is null then 0 else 1 end as current_time_to_event
                from final_view as a
                left join (
                select max(adds_days_to_event) as adds_days_to_event
                from final_view where event_type = 'current_ppv' and total_adds is not null
                ) as b
                on a.adds_days_to_event = b.adds_days_to_event
                left join (
                select distinct adds_days_to_event, adds_time_to_event
                from final_view where event_type = 'current_ppv' and total_adds is not null
                ) as c
                on a.adds_days_to_event = c.adds_days_to_event and a.adds_time_to_event = c.adds_time_to_event
        )
)),
actuals_estimates as
(select a.*,
b.event_reporting_type as current_event_reporting_type,
b.event_name as current_event_name,
b.event_date as current_event_date,
b.event_dttm as current_event_dttm,
b.current_adds_date,
b.current_adds_days_to_event,
b.current_adds_day_of_week,
b.current_adds_time,
b.ghw_adds_tillnow,
b.ghw_adds_estimate,
b.currentday_adds_tillnow,
b.currentday_adds_estimate,
b.weekend_adds_tillnow,
b.weekend_adds_estimate
from final_view as a
left join estimates as b
on a.adds_days_to_event = b.current_adds_days_to_event),
-- This table bring up next scheduled ppv date--
--next_event as
--(select top 1 trunc(event_dttm) as forecast_event_dt, 
--dateadd(day,-2,trunc(event_dttm)) as forecast_start_dt from cdm.dim_event 
--where trunc(event_dttm)>=getdate() and
--event_type_cd = 'PPV' order by event_dttm asc),
next_event as
(select top 1 event_date as forecast_event_dt, 
dateadd(day,-2,event_date) as forecast_start_dt from udl_nplus.raw_da_weekly_ppv_hourly_comps  where 
as_on_date in (select Max(as_on_date) from udl_nplus.raw_da_weekly_ppv_hourly_comps) order by event_date desc),
--To calculate daily Forecast--
forecast1 as 
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
--sum(paid_new_adds+paid_winbacks+trial_adds) as current_day_forecast
--TAB-2028
sum(paid_new_adds+paid_winbacks) as current_day_forecast
from {{source('fds_nplus','aggr_nplus_daily_forcast_output')}}
where forecast_date=(select max(forecast_date) from {{source('fds_nplus','aggr_nplus_daily_forcast_output')}})
and UPPER(payment_method)='MLBAM' and Upper(official_run_flag)='OFFICIAL' 
and trunc(bill_date) >= (select forecast_start_dt from next_event)
and trunc(bill_date) <= (select forecast_event_dt from next_event)
group by bill_date
order by bill_date),

forecast2 as
(select forecast_event_dt,
bill_date,
bill_day_of_week,
current_day_forecast
from next_event,
forecast1),
--To calculate Weekday Forecast--
forecast3 as
(select 
forecast_event_dt,
bill_day_of_week,
current_day_forecast
from forecast2 
--where trunc(bill_date) = current_date),
--TAB-2028
where trunc(bill_date) = date(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())))),

--To calculate Weekend Forecast--
forecast4 as 
(select 
forecast_event_dt,
sum(current_day_forecast) as weekend_forecast
from forecast2
group by forecast_event_dt),
--Merging Weekday and Weekend Forecast--
forecast_view as 
(select a.bill_day_of_week,
a.current_day_forecast,
b.weekend_forecast,
b.forecast_event_dt
from forecast3 as a
left join forecast4 as b
on a.forecast_event_dt=b.forecast_event_dt
),
--Merging Actuals, Estimates and Forecast data--
actuals_estimates_forecast_view as
(select 
a.*,
b.current_day_forecast,
b.weekend_forecast
from actuals_estimates as a 
left join 
forecast_view as b
on a.current_event_date=b.forecast_event_dt)
select a.*,'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_PPV' etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id,
    SYSDATE                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm from actuals_estimates_forecast_view a)
