
SELECT c.date as event_date,
       c.ppv_event_nm,
       c.ppv_nm,
       c.start_date as ghw_start_date,
       c.end_date as ghw_end_date,
       c.year,
       c.event,
       c.full_date,
       c.daily_paid_adds_cnt,
       c.daily_trial_adds_cnt,
       c.daily_promo_paid_add_cnt,
       c.daily_paid_adds_cnt_new,
       c.daily_paid_adds_cnt_winback,
       d.bill_date,
       d.paid_winbacks as forecast_paid_winbacks,
       d.new_paid as forecast_new_paid,
       d.free_trial_subs as forecast_free_trial_subs

  FROM
  (

SELECT a.* ,
       b.daily_paid_adds_cnt,b.daily_trial_adds_cnt,b.daily_promo_paid_add_cnt,
        b.daily_paid_adds_cnt_new,b.daily_paid_adds_cnt_winback 
 FROM
(

select date,ppv_event_nm,ppv_nm,
	       start_date,end_date,year,
		   event,full_date
FROM
        (		
          select trunc(event_dttm) as date,
               ppv_event_nm,ppv_nm,
               trunc(DATEADD(day, -6, date)) as start_date, 
               trunc(DATEADD(day,2, date)) as end_date,
               EXTRACT(YEAR FROM date) as year,
               CONCAT(ppv_event_nm,TO_CHAR(year,'9999')) as event
          from "entdwdb"."cdm"."dim_event"
		  
          where ppv_event_nm <> '' 
           and event_status='Published'
         )
         left join "entdwdb"."cdm"."dim_date"
                on start_date<=full_date and end_date>=full_date
    )a
    
  LEFT JOIN 
	(
	select as_on_date,
	sum(coalesce(daily_paid_adds_cnt,0)) as daily_paid_adds_cnt,
	sum(coalesce(daily_trial_adds_cnt,0)) as daily_trial_adds_cnt,
	sum(coalesce(daily_promo_paid_add_cnt,0)) as daily_promo_paid_add_cnt,
	sum(case when order_type='first' then coalesce(daily_new_adds_cnt,0) else null end) as daily_paid_adds_cnt_new,
	sum(case when order_type='winback' then coalesce(daily_new_adds_cnt,0) else null end) as daily_paid_adds_cnt_winback
	from "entdwdb"."fds_nplus"."aggr_daily_subscription"
	where payment_method in ('incomm' ,'paypal' ,'stripe' ,'unknown' ,'cybersource','roku_iap')
	group by 1 
	) b
on a.full_date=b.as_on_date-1 
)c

RIGHT JOIN
	(
                 select trunc(bill_date) as bill_date,                
                 sum(coalesce(paid_winbacks,0))+sum(coalesce(paid_new_with_trial,0))+sum(coalesce(trial_winback_adds,0)) as paid_winbacks,
                 sum(coalesce(paid_new_adds,0))+sum(coalesce(paid_new_with_Trial,0)) as new_paid,                
                 sum(coalesce(trial_adds,0)) as free_trial_subs
                 from "entdwdb"."fds_nplus"."aggr_nplus_daily_forcast_output"
                 where forecast_date=(select max(forecast_date) from "entdwdb"."fds_nplus"."aggr_nplus_daily_forcast_output")
                 and payment_method in ('mlbam','roku','apple') 
                 and official_run_flag='official' 
                 and bill_date>'2019-12-20'
                 group by 1 
                 order by 1
	 )d
 on c.full_date=d.bill_date
 where c.date is not null