{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select b.*, a.hours_watched as hours_watched_wk, a.hours_watched_tier2 as hours_watched_tier2_wk, a.active_network_subscribers as active_network_subscribers_wk, 
a.hours_per_tot_subscriber as hours_per_tot_subscriber_wk, a.active_viewers as views_wk, 0 as ad_impressions_wk, 
c.network_subscriber_adds as network_subscriber_adds_wk,
c.new_adds_direct_t3 + c.reg_prospects_t2_to_t3 as new_adds_wk, 
c.new_adds_direct_t3 as new_adds_direct_t3_wk,
c.reg_prospects_t2_to_t3 as reg_prospects_t2_to_t3_wk,
c.winback_adds_t2_to_t3 as winback_adds_t2_to_t3_wk,
c.lp_adds as lp_adds_wk,
c.new_free_version_regns as new_free_version_regns_wk,
c.network_losses as network_losses_wk,
'Network'::varchar(12) as platform,
'GLOBAL'::varchar(12) as type
from 
{{ref('intm_dim_dates')}} b
left join 
(       
        select a.monday_date, a.hours_watched, b.active_network_subscribers,
        a.hours_watched/b.active_network_subscribers as hours_per_tot_subscriber, 
        a.hours_watched_tier2,a.active_viewers
        from
        (select date_trunc('week',stream_start_dttm) as monday_date, 
        coalesce(round(sum(case when subs_tier = '3' then play_time end )/60),0) as hours_watched,
        coalesce(round(sum(case when subs_tier = '2' and trunc(stream_start_dttm) >= '2020-06-01' then play_time else 0 end )/60),0) as hours_watched_tier2,
        count(distinct(case when subs_tier = '2' and trunc(stream_start_dttm) >= '2020-06-01' then src_fan_id else null end)) as active_viewers  
        from {{source('fds_nplus','fact_daily_content_viewership')}} 
        where trunc(stream_start_dttm) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
        group by 1) a
        join
        (select a.as_on_date,b.cal_year_mon_week_begin_date as monday_date,
        sum(a.total_active_cnt) as active_network_subscribers
        from {{source('fds_nplus','AGGR_TOTAL_SUBSCRIPTION')}} a
        join {{ref('intm_dim_dates')}} b
        on a.as_on_date = b.cal_year_mon_week_end_date+1
        where a.as_on_date >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
        group by 1,2) b
        on a.monday_date=b.monday_date
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date
left join
(select date_trunc('week',as_on_date-1) as monday_date,
sum(daily_new_adds_cnt) as network_subscriber_adds,
sum(first_total_adds_new_to_t3 ) as new_adds_direct_t3,
sum(first_total_adds_upgrades) as reg_prospects_t2_to_t3,
sum(case when payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') and order_type = 'winback' then daily_new_adds_cnt else  0 end) as winback_adds_t2_to_t3,
sum(case when payment_method in ('china_pptv', 'osn', 'rogers', 'astro') then daily_new_adds_cnt  else 0 end) as lp_adds,
sum(case when as_on_date-1 >= '2020-06-01' then daily_tier2_prospect_loggedin_new_users_cnt else 0 end) as new_free_version_regns,
sum(daily_loss_cnt) as network_losses
from {{source('fds_nplus','aggr_daily_subscription')}}
where as_on_date >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
group by 1
) c
on trunc(c.monday_date) = b.cal_year_mon_week_begin_date