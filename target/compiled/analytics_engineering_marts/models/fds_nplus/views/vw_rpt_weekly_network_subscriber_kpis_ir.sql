
with curr_year as
(
select 
       cal_year,
       cal_year_week_num,
       cal_year_week_begin_date,
       cal_year_week_end_date,
       sum(paid) as paid,
       sum(lp_adds) as lp_adds,
       sum(free_trials) as free_trials,
       sum(losses) as losses,
       sum(case when rn=2 then paid_ending_actives else 0 end) as paid_ending_actives,
       sum(case when rn=2 then total_ending_actives else 0 end) as total_ending_actives,
       dense_rank() over(order by cal_year desc) as cyr_rn
from   
(
select a.*,
       row_number() over(partition by cal_year,cal_year_week_num order by as_on_date desc) as rn,
       cal_year,
       cal_year_week_num,
       cal_year_week_begin_date,
       cal_year_week_end_date  
from
(
select 
(as_on_date) as as_on_date
,sum(coalesce(daily_paid_adds_cnt,0))-sum(coalesce(daily_lp_add_cnt,0)) as paid
,sum(daily_lp_add_cnt) as lp_adds
,sum(daily_trial_adds_cnt) as free_trials
,sum(coalesce(daily_loss_cnt,0))+sum(coalesce(daily_lp_loss_cnt,0))+sum(coalesce(daily_iap_loss_cnt,0)) as losses
,sum(total_active_cnt) as total_ending_actives
,sum(total_paid_active_cnt) as paid_ending_actives
from fds_nplus.aggr_kpi_hist_prod
where as_on_date < '2019-12-14'
group by 1

union

select 
dly.as_on_date
,paid
,lp_adds
,free_trials
,losses
,total_ending_actives
,paid_ending_actives
from 
(
select 
(as_on_date) as as_on_date
,sum(case when payment_method not in ('astro','china_pptv','osn','rogers') then daily_paid_adds_cnt else 0 end) paid
,sum(case when payment_method in ('astro','china_pptv','osn','rogers') then daily_paid_adds_cnt else 0 end) lp_adds
,sum(daily_trial_adds_cnt) as free_trials
,sum(coalesce(daily_loss_cnt,0)) 
+ sum(coalesce(daily_iap_loss_cnt,0)) as losses
from fds_nplus.aggr_daily_subscription
where as_on_date >= '2019-12-14'
group by 1
) dly

left join
(
select 
(as_on_date) as as_on_date
,sum(coalesce(total_active_cnt,0)) as total_ending_actives
,sum(coalesce(total_paid_active_cnt,0)) as paid_ending_actives
from fds_nplus.aggr_total_subscription
where as_on_date >= '2019-12-14'
group by 1
) tot
on dly.as_on_date = tot.as_on_date
) a

left join
(
select  full_date+2 as full_date, 
        cal_year,
        cal_year_week_num,
        cal_year_week_begin_date+1 as cal_year_week_begin_date,
        cal_year_week_end_date+1 as cal_year_week_end_date
        from cdm.dim_date
) b
on a.as_on_date = b.full_date
) daily
group by 1,2,3,4
),

prev_year as
(
select       
       cal_year as prev_cal_year,
       cal_year_week_num as prev_cal_year_week_num,
       cal_year_week_begin_date as prev_cal_year_week_begin_date,
       cal_year_week_end_date as prev_cal_year_week_end_date,
       sum(paid) as prev_paid,
       sum(lp_adds) as prev_lp_adds,
       sum(free_trials) as prev_free_trials,
       sum(losses) as prev_losses,
       sum(case when rn=2 then paid_ending_actives else 0 end) as prev_paid_ending_actives,
       sum(case when rn=2 then total_ending_actives else 0 end) as prev_total_ending_actives,
       dense_rank() over(order by cal_year desc) as pyr_rn
from   
(
select a.*,
       row_number() over(partition by cal_year,cal_year_week_num order by as_on_date desc) as rn,
       cal_year,
       cal_year_week_num,
       cal_year_week_begin_date,
       cal_year_week_end_date  
from
(
select 
(as_on_date) as as_on_date
,sum(coalesce(daily_paid_adds_cnt,0))-sum(coalesce(daily_lp_add_cnt,0)) as paid
,sum(daily_lp_add_cnt) as lp_adds
,sum(daily_trial_adds_cnt) as free_trials
,sum(coalesce(daily_loss_cnt,0))+sum(coalesce(daily_lp_loss_cnt,0))+sum(coalesce(daily_iap_loss_cnt,0)) as losses
,sum(total_active_cnt) as total_ending_actives
,sum(total_paid_active_cnt) as paid_ending_actives
from fds_nplus.aggr_kpi_hist_prod
where as_on_date < '2019-12-14'
group by 1

union

select 
dly.as_on_date
,paid
,lp_adds
,free_trials
,losses
,total_ending_actives
,paid_ending_actives
from 
(
select 
(as_on_date) as as_on_date
,sum(case when payment_method not in ('astro','china_pptv','osn','rogers') then daily_paid_adds_cnt else 0 end) paid
,sum(case when payment_method in ('astro','china_pptv','osn','rogers') then daily_paid_adds_cnt else 0 end) lp_adds
,sum(daily_trial_adds_cnt) as free_trials
,sum(coalesce(daily_loss_cnt,0)) 
+ sum(coalesce(daily_iap_loss_cnt,0)) as losses
from fds_nplus.aggr_daily_subscription
where as_on_date >= '2019-12-14'
group by 1
) dly

left join
(
select 
(as_on_date) as as_on_date
,sum(coalesce(total_active_cnt,0)) as total_ending_actives
,sum(coalesce(total_paid_active_cnt,0)) as paid_ending_actives
from fds_nplus.aggr_total_subscription
where as_on_date >= '2019-12-14'
group by 1
) tot
on dly.as_on_date = tot.as_on_date
) a

left join
(
select  full_date+2 as full_date, 
        cal_year,
        cal_year_week_num,
        cal_year_week_begin_date+1 as cal_year_week_begin_date,
        cal_year_week_end_date+1 as cal_year_week_end_date
        from cdm.dim_date
) b
on a.as_on_date = b.full_date
) daily
where cal_year <> (select max(cal_year) from curr_year)
group by 1,2,3,4
)
select cal_year,cal_year_week_num,cal_year_week_begin_date,cal_year_week_end_date, paid, lp_adds,free_trials,losses, paid_ending_actives,total_ending_actives,
       prev_cal_year,prev_cal_year_week_num,prev_cal_year_week_begin_date,prev_cal_year_week_end_date, prev_paid, prev_lp_adds,prev_free_trials,prev_losses,prev_paid_ending_actives,prev_total_ending_actives
from 
curr_year left join prev_year 
on curr_year.cyr_rn =  prev_year.pyr_rn and curr_year.cal_year_week_num = prev_year.prev_cal_year_week_num
order by cal_year desc, cal_year_week_num asc