
select a.bill_date,a.paid_winbacks, a.new_paid, a.free_trial_subs, a.losses,
paid_churn_rate, total_churn_rate, eom_total_subs, adp,
paid_winbacks_ly, new_paid_ly, free_trial_subs_ly, losses_ly, paid_churn_rate_ly, total_churn_rate_ly, eom_total_subs_ly, adp_ly,
paid_winbacks_f, new_paid_f, free_trial_subs_f, losses_f, eom_total_subs_f, adp_f,
mnthly_total_hours_watched, mnthly_avg_hours_per_sub,lst_mnth_subs_viewing_cohort_rate,
mnthly_total_hours_watched_ly, mnthly_avg_hours_per_sub_ly, lst_mnth_subs_viewing_cohort_rate_ly 
from 
(
-------------------------------------------------------------------------------------------------------------------------------
--Current Year Actuals
-------------------------------------------------------------------------------------------------------------------------------
--OTT Gross Adds, Free Trials, Paid Winbacks, New Paid,Losses
(select 
to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(nvl(paid_winbacks,0)+nvl(paid_new_with_trial,0)) as paid_winbacks,
sum(nvl(paid_new_adds,0)-nvl(paid_new_with_trial,0)) as new_paid,sum(trial_adds) as free_trial_subs,
sum(nvl(paid_losses_actual,0))+sum(nvl(total_trial_loss,0)) as losses 
from fds_nplus.aggr_nplus_monthly_forcast_output 
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output)  
and payment_method in ('mlbam','apple','roku')
and official_run_flag='official' 
and extract(year from (dateadd(month,-1,current_date))) =year
and extract(month from current_date)-1=month
group by 1,2) a

left join

--OTT Total Churn Rate, Paid Churn Rate
(select 
to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(paid_losses_actual)/sum(paid_active_start) :: float as paid_churn_rate,
sum(nvl(paid_losses_actual,0)+nvl(total_trial_loss,0))/(sum(nvl(paid_active_start,0))+ 
max(nvl(c.trial_active_2,0))+max(nvl(b.trial_active,0))) :: float as total_churn_rate
from fds_nplus.aggr_nplus_monthly_forcast_output a
left join (select sum(total_trial_active_cnt) trial_active from fds_nplus.aggr_total_subscription 
where as_on_date=date_trunc('month',dateadd(month,-1,current_date)) and payment_method<>'roku_iap') b
on 1=1 
left join (select sum(trial_active_end) as trial_active_2
from fds_nplus.aggr_nplus_monthly_forcast_output where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output) 
and payment_method in ('roku')
and official_run_flag='official' 
and extract(year from (dateadd(month,-2,current_date))) =year
and extract(month from current_date)-2=month ) c
on 1=1
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output) 
and payment_method in ('mlbam','apple','roku')
and official_run_flag='official' 
and extract(year from (dateadd(month,-1,current_date))) =year
and extract(month from current_date)-1=month
group by 1,2 
) b
on a.bill_date = b.bill_date

left join

(
--All Providers EOM,ADP
select 
to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(nvl(paid_active_end,0))+sum(case when payment_method='roku' then nvl(trial_active_end,0) else 0 end)+max(nvl(b.trial_active,0)) as eom_total_subs,
sum(avg_daily_paid_subs) as adp
from fds_nplus.aggr_nplus_monthly_forcast_output a
left join (select sum(total_trial_active_cnt) trial_active from fds_nplus.aggr_total_subscription 
where as_on_date=date_trunc('month',current_date) and payment_method<>'roku_iap') b
on 1=1 
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output) 
and official_run_flag='official' 
and extract(year from (dateadd(month,-1,current_date))) =year
and extract(month from current_date)-1=month
group by 1,2
)c
on a.bill_date = c.bill_date

left join
(
-------------------------------------------------------------------------------------------------------------------------------
--Previous Year Actuals
-------------------------------------------------------------------------------------------------------------------------------
--OTT Gross Adds, Free Trials, Paid Winbacks, New Paid,Losses
select 
to_date((cast(year+1 as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(nvl(paid_winbacks,0)+nvl(paid_new_with_trial,0)) as paid_winbacks_ly,
sum(nvl(paid_new_adds,0)-nvl(paid_new_with_trial,0)) as new_paid_ly,sum(trial_adds) as free_trial_subs_ly,
sum(nvl(paid_losses_actual,0))+sum(nvl(total_trial_loss,0)) as losses_ly 
from fds_nplus.aggr_nplus_monthly_forcast_output 
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output)
and payment_method in ('mlbam','apple','roku')
and official_run_flag='official' 
and extract(year from dateadd(year,-1,(dateadd(month,-1,current_date)))) =year
and extract(month from current_date)-1=month
group by 1,2
) d
on a.bill_date = d.bill_date

left join
(
--OTT Total Churn Rate, Paid Churn Rate
select 
to_date((cast(year+1 as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(paid_losses_actual)/sum(paid_active_start) :: float as paid_churn_rate_ly,
sum(nvl(paid_losses_actual,0)+nvl(total_trial_loss,0))/(sum(nvl(paid_active_start,0))+ 
max(nvl(c.trial_active_2,0))+max(nvl(b.trial_active,0))) :: float as total_churn_rate_ly
from fds_nplus.aggr_nplus_monthly_forcast_output a
left join 
(select sum(trial_active) as trial_active from (select sum(total_trial_active_cnt) trial_active from fds_nplus.aggr_total_subscription 
where as_on_date=date_trunc('month',dateadd(year,-1,dateadd(month,-1,current_date))) and payment_method<>'roku_iap'
and as_on_date>'2019-12-14'
union all
select sum(total_trial_active_cnt)-sum(total_iap_trial_active_cnt) trial_active from fds_nplus.aggr_kpi_hist
where as_on_date= date_trunc('month',dateadd(year,-1,dateadd(month,-1,current_date))) and as_on_Date<='2019-12-14')) b
on 1=1 
left join (select sum(trial_active_end) as trial_active_2
from fds_nplus.aggr_nplus_monthly_forcast_output where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output) 
and payment_method in ('roku')
and official_run_flag='official' 
and extract(year from dateadd(year,-1,(dateadd(month,-1,current_date)))) =year
and extract(month from current_date)-2=month) c
on 1=1
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output) 
and payment_method in ('mlbam','apple','roku')
and official_run_flag='official' 
and extract(year from dateadd(year,-1,(dateadd(month,-1,current_date)))) =year
and extract(month from current_date)-1=month
group by 1,2
) e
on a.bill_date = e.bill_date

left join
(
--All Providers EOM,ADP
select 
to_date((cast(year+1 as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(nvl(paid_active_end,0))+sum(case when payment_method='roku' then trial_active_end else 0 end)+max(nvl(b.trial_active,0)) as eom_total_subs_ly,
sum(avg_daily_paid_subs) as adp_ly
from fds_nplus.aggr_nplus_monthly_forcast_output a
left join (select sum(trial_active) as trial_active from (select sum(total_trial_active_cnt) trial_active from fds_nplus.aggr_total_subscription 
where as_on_date=date_trunc('month',dateadd(year,-1,current_date)) and payment_method<>'roku_iap'
and as_on_date>'2019-12-14'
union all
select sum(total_trial_active_cnt)-sum(total_iap_trial_active_cnt) trial_active from fds_nplus.aggr_kpi_hist
where as_on_date= date_trunc('month',dateadd(year,-1,current_date)) and as_on_Date<='2019-12-14')) b
on 1=1 
where forecast_date=(select max(forecast_date) from fds_nplus.aggr_nplus_monthly_forcast_output) 
and official_run_flag='official' 
and extract(year from dateadd(year,-1,(dateadd(month,-1,current_date)))) =year
and extract(month from current_date)-1=month
group by 1,2
) f
on a.bill_date = f.bill_date

left join

(
-------------------------------------------------------------------------------------------------------------------------------
--Forecast Current Year
-------------------------------------------------------------------------------------------------------------------------------
--OTT FYE (OTT Gross Adds, Free Trials, Paid Winbacks, New Paid,Losses)
select 
to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(nvl(paid_winbacks,0)+nvl(paid_new_with_trial,0)) as paid_winbacks_f,
sum(nvl(paid_new_adds,0)-nvl(paid_new_with_trial,0)) as new_paid_f,sum(trial_adds) as free_trial_subs_f,
sum(nvl(paid_losses_actual,0))+sum(nvl(total_trial_loss,0)) as losses_f
from fds_nplus.aggr_nplus_monthly_forcast_output 
where forecast_date=date_trunc('month',(dateadd(month,-1,current_date)))
and payment_method in ('mlbam','apple','roku')
and official_run_flag='official' 
and extract(year from (dateadd(month,-1,current_date))) =year
and extract(month from current_date)-1=month
group by 1,2
) g
on a.bill_date = g.bill_date

left join
(
--All Providers FYE EOM,ADP
select 
to_date((cast(year as char(4)) + '-' + cast(month as char(2)) + '-01'),'yyyy-mm-dd') as bill_date,forecast_date,
sum(nvl(paid_active_end,0))+sum(nvl(trial_active_end,0)) as eom_total_subs_f,
sum(avg_daily_paid_subs) as adp_f
from fds_nplus.aggr_nplus_monthly_forcast_output  
where forecast_date=date_trunc('month',(dateadd(month,-1,current_date)))
and official_run_flag='official' 
and extract(year from (dateadd(month,-1,current_date))) =year
and extract(month from current_date)-1=month
group by 1,2
) h
on a.bill_date = h.bill_date

left join
(
--Viewership Actual
select  mnth_start_dt as bill_date, 
mnthly_total_hours_watched,mnthly_avg_hours_per_sub
from fds_nplus.aggr_monthly_program_type_viewership
where stream_type_cd='live+vod'
and program_type_cd='All'
and as_on_date=(select max(as_on_date) from fds_nplus.aggr_monthly_program_type_viewership)
and subs_tier='95'
and initial_signup_year = '2099'
) i
on a.bill_date = i.bill_date

left join
(
select  mnth_start_dt as bill_date,lst_mnth_subs_viewing_cohort_rate
from fds_nplus.aggr_monthly_subs_cohort_viewership
where stream_type_cd='live+vod'
and program_type_cd='All'
and as_on_date=(select max(as_on_date) from fds_nplus.aggr_monthly_subs_cohort_viewership)
and subs_tier='95'
and subs_year = '2099'
) j
on a.bill_date = j.bill_date

left join
(
--Viewership Last Year
select  trunc(dateadd(year,1,mnth_start_dt)) as bill_date, 
mnthly_total_hours_watched as mnthly_total_hours_watched_ly,
mnthly_avg_hours_per_sub as mnthly_avg_hours_per_sub_ly
from fds_nplus.aggr_monthly_program_type_viewership  
where stream_type_cd='live+vod'
and program_type_cd='All'
and subs_tier='95'
and initial_signup_year = '2099'
and extract(year from dateadd(year,-1,(dateadd(month,-1,current_date)))) = extract(year from mnth_start_dt)
and extract(month from current_date)-1=extract(month from mnth_start_dt)
) k
on a.bill_date = k.bill_date

left join
(
select  trunc(dateadd(year,1,mnth_start_dt)) as bill_date,
lst_mnth_subs_viewing_cohort_rate as lst_mnth_subs_viewing_cohort_rate_ly
from fds_nplus.aggr_monthly_subs_cohort_viewership
where stream_type_cd='live+vod'
and program_type_cd='All'
and subs_tier='95'
and subs_year = '2099'
and extract(year from dateadd(year,-1,(dateadd(month,-1,current_date)))) = extract(year from mnth_start_dt)
and extract(month from current_date)-1=extract(month from mnth_start_dt)
) l
on a.bill_date = l.bill_date
)