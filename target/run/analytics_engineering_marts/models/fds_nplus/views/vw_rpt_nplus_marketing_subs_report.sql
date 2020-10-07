

  create view "entdwdb"."fds_nplus"."vw_rpt_nplus_marketing_subs_report__dbt_tmp" as (
    -- Marketing Subs Report View

/*
*************************************************************************************************************************************************
   Date        : 08/14/2020
   Version     : 1.0
   TableName   : vw_rpt_nplus_marketing_subs_report
   Schema	   : fds_nplus
   Contributor : Hima Dasan
   Description : vw_rpt_nplus_marketing_subs_report view consist of Actuals,forecast and Budget for adds and Disconnects For Roku,Apple and mlbam (Monthly)
*************************************************************************************************************************************************
*/

 

select a.bill_date,a.free_adds_budget,a.paid_adds_budget,a.free_disconnects_budget,a.paid_disconnects_budget,
 b.free_adds_actuals,b.paid_adds_actuals,b.free_disconnects_actuals,b.paid_disconnects_actuals,
 case when a.bill_Date < '2019-12-01' then e.EOP_Subs  else f.EOP_Subs end  as EOP_Subs ,
  c.free_adds_FYE,c.paid_adds_FYE,c.free_disconnects_FYE,c.paid_disconnects_FYE
 from ( 
 
 (select  to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,
 forecast_date,year,month,
      ROUND(sum (trial_adds)) as free_adds_budget,
       round(sum(paid_new_adds)) + round(sum(paid_winbacks)) as paid_adds_budget,
       case when year='2018' and month ='1' then '23974'
	   when year='2018' and month ='2' then '66640'
	   when year='2018' and month ='3' then '11277'
	   when year='2018' and month ='4' then '31978'
	   when year='2018' and month ='5' then '116441'
	   when year='2018' and month ='6' then '8326'
	   when year='2018' and month ='7' then '32396'
	   when year='2018' and month ='8' then '14618'
	   when year='2018' and month ='9' then '33469'
	   when year='2018' and month ='10' then '9547'
	   when year='2018' and month ='11' then '14309'
	   when year='2018' and month ='12' then '15939'
		   else round(sum(total_trial_loss)) end as free_disconnects_budget,
       round(sum(paid_losses_actual)) as paid_disconnects_budget
	  from "entdwdb"."fds_nplus"."aggr_nplus_monthly_forcast_output"  
 where payment_method in ('apple','roku','mlbam')
 and official_run_flag ='official'
 and comments='jan_budget'
  and year between extract(year from (dateadd(month,-1,current_date))) -3 and 
  extract(year from (dateadd(month,-1,current_date)))
and forecast_date = to_date((cast(year as char(4)) + '-01-01'),'yyyy-mm-dd') 
 group by 1,2,3,4
 ) a 
left join (select as_on_date,sum(total_active_cnt) as EOP_Subs from  "entdwdb"."fds_nplus"."aggr_kpi_hist"  group by 1) e on 
 a.bill_date= dateadd(month,-1,e.as_on_date)
 left join (select as_on_date,sum(total_active_cnt) as EOP_Subs from "entdwdb"."fds_nplus"."aggr_total_subscription"   group by 1) f on
 a.bill_date= dateadd(month,-1,f.as_on_date)
 left join
(select
to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,
forecast_date,year,  month,
      round(sum (trial_adds)) as free_adds_actuals,
       round(sum(paid_new_adds)) + round(sum(paid_winbacks)) as paid_adds_actuals,
       round(sum(total_trial_loss)) as free_disconnects_actuals,
       round(sum(paid_losses_actual)) as paid_disconnects_actuals
 from "entdwdb"."fds_nplus"."aggr_nplus_monthly_forcast_output" a
 where payment_method in ('apple','roku','mlbam')
 and official_run_flag ='official' 
 and forecast_date= (select max(forecast_date) from "entdwdb"."fds_nplus"."aggr_nplus_monthly_forcast_output" )
 and (year||case when length(month) <2 then ('0' + cast(month as varchar))
 else cast(month as varchar) end) < to_char((select max(forecast_date) from 
 "entdwdb"."fds_nplus"."aggr_nplus_monthly_forcast_output" ), 'YYYYMM')
 group by 1,2,3,4) b
 on a.bill_date = b.bill_date
 
 LEFT JOIN
 (
 select  to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,
 forecast_date,year,month,
      round(sum (trial_adds)) as free_adds_FYE,
       round(sum(paid_new_adds)) + round(sum(paid_winbacks)) as paid_adds_FYE,
	   case when year='2018' and month ='1' then '23974'
	   when year='2018' and month ='2' then '64263'
	   when year='2018' and month ='3' then '25116'
	   when year='2018' and month ='4' then '25791'
	   when year='2018' and month ='5' then '156463'
	   when year='2018' and month ='6' then '8345'
	   when year='2018' and month ='7' then '17285'
	   when year='2018' and month ='8' then '12532'
	   when year='2018' and month ='9' then '31342'
	   when year='2018' and month ='10' then '15455'
	   when year='2018' and month ='11' then '18825'
	   when year='2018' and month ='12' then '17960'
       else round(sum(total_trial_loss)) end  as free_disconnects_FYE,
       round(sum(paid_losses_actual)) as paid_disconnects_FYE
	  -- (total_active_cnt) as EOP_subs
 from "entdwdb"."fds_nplus"."aggr_nplus_monthly_forcast_output"   
 where payment_method in ('apple','roku','mlbam')
 and official_run_flag ='official'
and to_date(add_months(forecast_date, -0),'yyyy-mm-01') = bill_date 
 and year = extract(year from bill_date)
 and month = extract(month from bill_date)
 group by 1,2,3,4) c
 on a.BILL_dATE = c.bill_Date
 )
 order by bill_date desc
  ) ;
