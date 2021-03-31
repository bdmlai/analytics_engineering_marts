	-- Marketing Subs Report View

/*
*************************************************************************************************************************************************
   Date        : 03/15/2021
   Version     : 1.0
   TableName   : vw_rpt_nplus_monthly_marketing_subs
   Schema	   : fds_nplus
   Contributor : Hima Dasan
   Description : vw_rpt_nplus_monthly_marketing_subs view consist of Actuals,forecast and Budget for adds and Disconnects For Roku,Apple and mlbam (Monthly)
*************************************************************************************************************************************************
*/

 {{
  config({
	"schemas": 'fds_nplus',	
	"materialized": 'view',"persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on fds_nplus.vw_rpt_nplus_monthly_marketing_subs to public'

		})
}}

select d.bill_date,d.country_region,a.new_adds_budget,a.Winback_Adds_budget,a.free_adds_budget,a.paid_adds_budget,a.free_disconnects_budget,a.paid_disconnects_budget,
 b.new_adds_Actuals,b.Winback_Adds_Actuals,b.free_adds_actuals,b.paid_adds_actuals,b.free_disconnects_actuals,b.paid_disconnects_actuals,
 b.EOP_Subs  as EOP_Subs ,
  c.new_adds_FYE,c.Winback_Adds_FYE,c.free_adds_FYE,c.paid_adds_FYE,c.free_disconnects_FYE,c.paid_disconnects_FYE
 from ( 
 (
  select  distinct to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,
 year,month,country_region from {{source('fds_nplus','aggr_nplus_monthly_forcast_output')}}  
  where payment_method in ('apple','roku','mlbam','google iap')
 and official_run_flag ='official'
 and year between '2017' and  extract(year from (dateadd(month,-1,current_date)))
 ) d  
 left join 
 (select  to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,
 forecast_date,year,month,country_region,
  case when bill_date >= '2020-06-01' then cast(round(sum(paid_new_adds)) + round(sum(trial_new_adds)) - round(sum(paid_new_with_trial)) as varchar)
else 'N/A' END  as new_adds_budget,
  case when bill_date >= '2020-06-01' THEN cast(round(sum(paid_winbacks)) + round(sum(trial_winback_adds)) + round(sum(paid_new_with_trial)) as varchar)
  else 'N/A' END  as Winback_Adds_budget,
      ROUND(sum (trial_adds)) as free_adds_budget,
       round(sum(paid_new_adds)) + round(sum(paid_winbacks)) as paid_adds_budget,
       round(sum(total_trial_loss))  as free_disconnects_budget,
       round(sum(paid_losses_actual)) as paid_disconnects_budget
	  from {{source('fds_nplus','aggr_nplus_monthly_forcast_output')}} 
 where payment_method in ('apple','roku','mlbam','google iap')
 and official_run_flag ='official'
 and comments='jan_budget'
and forecast_date = to_date((cast(year as char(4)) + '-01-01'),'yyyy-mm-dd') 
 group by 1,2,3,4,5
 ) a  on d.bill_date = a.bill_date and d.country_region=a.country_region
 /* left join (select as_on_date,sum(total_active_cnt) as EOP_Subs from {{source('fds_nplus','aggr_total_subscription')}}  group by 1) f on
 d.bill_date= dateadd(month,-1,f.as_on_date) */
 left join
(select
to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,
forecast_date,year,  month,country_region,
  case when bill_date >= '2020-06-01' then CAST(round(sum(paid_new_adds)) + round(sum(trial_new_adds)) - round(sum(paid_new_with_trial)) as varchar)
else 'N/A' END  as new_adds_Actuals,
 case when bill_date >= '2020-06-01' then  cast(round(sum(paid_winbacks)) + round(sum(trial_winback_adds)) + round(sum(paid_new_with_trial)) as varchar)
 else 'N/A' END as Winback_Adds_Actuals,
      round(sum (trial_adds)) as free_adds_actuals,
       round(sum(paid_new_adds)) + round(sum(paid_winbacks)) as paid_adds_actuals,
       round(sum(total_trial_loss)) as free_disconnects_actuals,
       round(sum(paid_losses_actual)) as paid_disconnects_actuals,
 round(sum(total_active_end)) as EOP_Subs
 from {{source('fds_nplus','aggr_nplus_monthly_forcast_output')}} a
 where payment_method in ('apple','roku','mlbam','google iap')
 and official_run_flag ='official' 
 and forecast_date= (select max(forecast_date) from {{source('fds_nplus','aggr_nplus_monthly_forcast_output')}} )
 and (year||case when length(month) <2 then ('0' + cast(month as varchar))
 else cast(month as varchar) end) < to_char((select max(forecast_date) from 
{{source('fds_nplus','aggr_nplus_monthly_forcast_output')}} ), 'YYYYMM')
 group by 1,2,3,4,5) b
 on d.bill_date = b.bill_date and d.country_region=b.country_region
 
 LEFT JOIN
 (
 select  to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,
 forecast_date,year,month,country_region,
  case when bill_date >= '2020-06-01' then CAST(round(sum(paid_new_adds)) + round(sum(trial_new_adds)) - round(sum(paid_new_with_trial)) AS VARCHAR)
  else 'N/A' END as new_adds_FYE,
  case when bill_date >= '2020-06-01' then CAST(round(sum(paid_winbacks)) + round(sum(trial_winback_adds)) + round(sum(paid_new_with_trial)) AS VARCHAR)
  else 'N/A' END as Winback_Adds_FYE,
      round(sum (trial_adds)) as free_adds_FYE,
       round(sum(paid_new_adds)) + round(sum(paid_winbacks)) as paid_adds_FYE,
	  round(sum(total_trial_loss))   as free_disconnects_FYE,
       round(sum(paid_losses_actual)) as paid_disconnects_FYE
 from {{source('fds_nplus','aggr_nplus_monthly_forcast_output')}} 
 where payment_method in ('apple','roku','mlbam','google iap')
 and official_run_flag ='official'
and to_date(add_months(forecast_date, -0),'yyyy-mm-01') = bill_date 
 and year = extract(year from bill_date) 
 and month = extract(month from bill_date)
 group by 1,2,3,4,5) c
 on d.BILL_dATE = c.bill_Date and d.country_region=c.country_region
 )
 order by bill_date desc
