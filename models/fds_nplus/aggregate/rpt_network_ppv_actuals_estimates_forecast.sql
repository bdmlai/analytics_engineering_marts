 {{
  config({
	"schema": 'fds_nplus',	
	"materialized": 'incremental',
	"pre-hook":["delete from fds_nplus.rpt_network_ppv_actuals_estimates_forecast",
"drop table if exists #final_view;
create table  #final_view as 
select 
event_date, event_dttm, event_name, event_type, event_reporting_type, 
adds_days_to_event, adds_day_of_week, adds_date, adds_time - extract(hour from event_dttm) as adds_time_to_event,
adds_time, paid_adds, trial_adds, total_adds
from dt_prod_support.rpt_ppv_final_table where adds_time is not null
union all
        (select a.event_date, a.event_dttm, a.event_name, a.event_type, a.event_reporting_type, 
        a.adds_days_to_event, a.adds_day_of_week, a.adds_date, b.adds_time - extract(hour from a.event_dttm) as adds_time_to_event,
        b.adds_time, b.paid_adds, b.trial_adds, b.total_adds
        from dt_prod_support.rpt_ppv_final_table as a
        inner join (Select  adds_date,	
	hour as adds_time,
	count(distinct case when flag = 'Paid' then customerid end) as  paid_adds,
	count(distinct case when flag = 'Trial' or flag = 'NA' then customerid end) as  trial_adds,(paid_adds+trial_adds)  total_adds from
           (Select customerid,	
			trunc(CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp))) as adds_date,	
            min(extract(hour from CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp)))) as hour,	
            case when payload_data_voucher_code is not null and payload_data_voucher = true and payload_data_price_with_tax_amount =0 and 
				payload_data_renewal = 'false' then 'Trial'	
                when payload_data_is_trial='true' or payload_data_price_with_tax_amount =0  then 'Trial'	
                when payload_data_is_trial='false' then 'Paid' else 'NA' end as flag
           from udl_nplus.stg_dice_stream_flattened	
           where payload_data_ta in ('SUCCESSFUL_PURCHASE')	
           and (payload_data_voucher_code is null or payload_data_voucher_code!='WWE Network VIP')	
           and (payload_data_renewal !='true' or payload_data_renewal is null)	
           and trunc(CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp))) = trunc(CONVERT_TIMEZONE('AMERICA/NEW_YORK', sysdate))
           group by 1,2,4) group by 1,2
	) as b
        on a.adds_date = b.adds_date
        where a.adds_time is null
);",

"drop table if exists #estimates;
create table #estimates as 
select *,(((ghw_adds_tillnow1+currentday_adds_tillnow)/d2_pct1)*(d2_pct2-d2_pct1))+currentday_adds_tillnow  currentday_adds_estimate,
(((ghw_adds_tillnow1+currentday_adds_tillnow)/d2_pct1)*(1-d2_pct1))+currentday_adds_tillnow  weekend_adds_estimate
from 
(select event_reporting_type, 
event_name,
event_date,
event_dttm,
current_adds_date, 
current_adds_days_to_event,
current_adds_day_of_week,
current_adds_time,
ghw_adds_tillnow,
currentday_adds_tillnow,
weekend_adds_tillnow,
ghw_adds_tillnow1,
(select d2_pct from dt_prod_support.rpt_ppv_hourly_pct where adds_time=current_adds_time and adds_day_of_week=current_adds_day_of_week) d2_pct1,
(select d2_pct from dt_prod_support.rpt_ppv_hourly_pct where adds_time=23 and adds_day_of_week=current_adds_day_of_week) d2_pct2,
case when ghw_adds_tillnow > 0 and comp_ghw_adds_tillnow > 0 then ghw_adds_tillnow/(comp_ghw_adds_tillnow/comp_ghw_adds) else -1 end as ghw_adds_estimate
from

        (select
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
        sum(case when event_type = 'current_ppv' and adds_days_to_event between -6 and -1 then total_adds::float else 0 end) as ghw_adds_tillnow1,
        sum(case when current_days_to_event = 1  and event_type = 'current_ppv' then total_adds::float else 0 end) as currentday_adds_tillnow,
		sum(case when current_time_to_event = 1 and event_type != 'current_ppv' and adds_days_to_event between -6 and 0 then total_adds::float else 0 end) as comp_ghw_adds_tillnow,
		sum(case when event_type != 'current_ppv' and adds_days_to_event between -6 and 0 then total_adds::float else 0 end) as comp_ghw_adds
		
        from (
                select a.*, 
                case when b.adds_days_to_event is null then 0 else 1 end as current_days_to_event,
                case when c.adds_time_to_event is null then 0 else 1 end as current_time_to_event
                from #final_view as a
                left join (
                select max(adds_days_to_event) as adds_days_to_event
                from #final_view where event_type = 'current_ppv' and total_adds is not null
                ) as b
                on a.adds_days_to_event = b.adds_days_to_event
                left join (
                select distinct adds_days_to_event, adds_time_to_event
                from #final_view where event_type = 'current_ppv' and total_adds is not null
                ) as c
                on a.adds_days_to_event = c.adds_days_to_event and a.adds_time_to_event = c.adds_time_to_event
        )) a);",
"drop table if exists #actuals_estimates;
create table #actuals_estimates as
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
from #final_view as a
left join #estimates as b
on a.adds_days_to_event = b.current_adds_days_to_event);",
"drop table if exists #forecast;
create table #forecast as
(select trunc(bill_date) as bill_date,forecast_event_dt,
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
from fds_nplus.aggr_nplus_daily_forcast_output a,
(select top 1 event_date as forecast_event_dt, 
dateadd(day,-2,event_date) as forecast_start_dt 
from udl_nplus.raw_da_weekly_ppv_hourly_comps_new where event_type='current_ppv'
and as_on_date=(select max(as_on_date) from udl_nplus.raw_da_weekly_ppv_hourly_comps_new)
and  update_date=(select max(update_date) from udl_nplus.raw_da_weekly_ppv_hourly_comps_new)) b
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_daily_forcast_output)
and UPPER(payment_method) in ('MLBAM','APPLE','ROKU') and Upper(official_run_flag)='OFFICIAL' 
and trunc(bill_date) >= b.forecast_start_dt
and trunc(bill_date) <= b.forecast_event_dt
group by bill_date,forecast_event_dt);",
"
--Merging Weekday and Weekend Forecast--
drop table if exists #forecast_view;
create table #forecast_view as
select bill_day_of_week,current_day_forecast,weekend_forecast,forecast_event_dt from
#forecast,(select sum(current_day_forecast) as weekend_forecast from #forecast)
where trunc(bill_date) = date(dateadd('hour',-1,convert_timezone('AMERICA/NEW_YORK', getdate())));",

"drop table if exists #actuals_estimates_forecast_view;
create table #actuals_estimates_forecast_view as
(select 
a.*,
b.current_day_forecast,
b.weekend_forecast
from #actuals_estimates as a 
left join 
#forecast_view as b
on a.current_event_date=b.forecast_event_dt);"]})}}
select a.*,'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', sysdate),'YYYY_MM_DD_HH_MI_SS')+'_PPV' etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id,
    convert_timezone('AMERICA/NEW_YORK', sysdate)                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm from #actuals_estimates_forecast_view a