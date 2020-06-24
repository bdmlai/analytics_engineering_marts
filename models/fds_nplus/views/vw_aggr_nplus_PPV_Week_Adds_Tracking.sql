 {{
  config({
    "schemas": 'fds_nplus',
	"materialized": 'view',
	"post-hook": ["COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.event_date IS 'Date and timestamp of the scheduled live events as received from LES';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.ppv_event_nm IS 'Name of the PPVevent';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.ppv_nm IS 'Name of the PPV';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.ghw_start_date IS 'date of Event Date minus 6 days';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.ghw_end_date IS 'date of Event date plus 2 days';  
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.year IS 'Extracted year from event date';  
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.event IS 'concatenated text of PPV event name and year ';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.full_date IS 'full date';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.daily_paid_adds_cnt IS 'Daily count of paid subscriber adds';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.daily_trial_adds_cnt IS 'Daily count of trial subscriber adds';  
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.daily_promo_paid_add_cnt IS 'Daily count of total promo paid subscription add';  
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.daily_paid_adds_cnt_new IS 'custom filed based on order type and Daily count of paid subscriber adds';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.daily_paid_adds_cnt_winback IS 'custom filed based on order type winback and Daily count of paid subscriber adds';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.bill_date IS 'Process date';
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.forecast_paid_winbacks IS 'Roll up of all paid winback subscriptions for the day';  
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.forecast_new_paid IS 'Roll up of all paid subscriptions who have paid for the first time';  
					COMMENT ON COLUMN fds_nplus.vw_aggr_nplus_PPV_Week_Adds_Tracking.forecast_free_trial_subs IS 'Total trial adds for the day';
					
			"]
	})
}}


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
          from {{source('cdm','dim_event')}}
          where ppv_event_nm <> '' 
           and event_status='Published'
         )
         left join {{source('cdm','dim_date')}}
                on start_date<=full_date and end_date>=full_date
    )a
    
  LEFT JOIN 
	(
	select as_on_date,
	sum(daily_paid_adds_cnt) as daily_paid_adds_cnt,
	sum(daily_trial_adds_cnt) as daily_trial_adds_cnt,
	sum(daily_promo_paid_add_cnt) as daily_promo_paid_add_cnt,
	sum(case when order_type='first' then daily_paid_adds_cnt else null end) as daily_paid_adds_cnt_new,
	sum(case when order_type='winback' then daily_paid_adds_cnt else null end) as daily_paid_adds_cnt_winback
	from {{source('fds_nplus','aggr_daily_subscription')}}
	where
	payment_method in ('incomm' ,'paypal' ,'stripe' ,'unknown' ,'cybersource')
	group by 1 
	) b
on a.full_date=b.as_on_date-1 
)c

RIGHT JOIN
	(
                 select trunc(bill_date) as bill_date,                --
                 sum(paid_winbacks)+sum(paid_new_with_trial) as paid_winbacks,
                 sum(paid_new_adds)-sum(paid_new_with_Trial) as new_paid,                --
                -- sum(paid_winbacks) as paid_winbacks,
                        --sum(paid_new_adds) as new_paid,
                        sum(trial_adds) as free_trial_subs
                 from {{source('fds_nplus','aggr_nplus_daily_forcast_output')}}
                 where forecast_date=(select max(forecast_date) from {{source('fds_nplus','aggr_nplus_daily_forcast_output')}})
                 and payment_method in ('mlbam','roku','apple') 
                 and official_run_flag='official' 
                 and bill_date>'2019-12-20'
                 group by 1 
                 order by 1
	 )d
 on c.full_date=d.bill_date
 where c.date is not null