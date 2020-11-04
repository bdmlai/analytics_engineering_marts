 {{
  config({
	"schema": 'fds_cp',	
	"materialized": 'incremental',
	"pre-hook": ["delete from fds_cp.rpt_cp_weekly_consolidated_kpi",
	"
	--create dates for rollup
drop table if exists #dim_dates;
create table #dim_dates as
select distinct cal_year, extract('month' from cal_year_mon_week_begin_date) as cal_mth_num, 
case when cal_year_week_num_mon is null then 1 else cal_year_week_num_mon end as cal_year_week_num_mon,
cal_year_mon_week_begin_date, cal_year_mon_week_end_date
from cdm.dim_date where cal_year_mon_week_begin_date >= trunc(dateadd('year',-2,date_trunc('year',getdate()))) and cal_year_mon_week_end_date < date_trunc('week',getdate());

--create network weekly dataset
drop table if exists #dp_wkly_nw;
create table #dp_wkly_nw as
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
'Network' as platform,
'Network' as type
from 
#dim_dates b
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
        from fds_nplus.fact_daily_content_viewership 
        where trunc(stream_start_dttm) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
        group by 1) a
        join
        (select a.as_on_date,b.cal_year_mon_week_begin_date as monday_date,sum(a.total_active_cnt) as active_network_subscribers
        from fds_nplus.AGGR_TOTAL_SUBSCRIPTION a
        join #dim_dates b
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
from fds_nplus.aggr_daily_subscription 
where as_on_date >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
group by 1
) c
on trunc(c.monday_date) = b.cal_year_mon_week_begin_date
;

--create facebook weekly dataset
drop table if exists #dp_wkly_fb;
create table #dp_wkly_fb as
select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, 0 as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'Facebook' as platform,
'Facebook' as type
from 
#dim_dates b
left join 
(       
select monday_date,views,minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Facebook'
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;

--create TikTok weekly dataset
drop table if exists #dp_wkly_tt;
create table #dp_wkly_tt as
select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, 0 as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'TikTok' as platform,
'TikTok' as type
from 
#dim_dates b
left join 
(       
select monday_date,views,minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'TikTok'
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;

--create dotocm weekly dataset
drop table if exists #dp_wkly_dc;
create table #dp_wkly_dc as
select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, 0 as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'Dotcom' as platform,
'Dotcom' as type
from 
#dim_dates b
left join 
(       
select monday_date,views,minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = '.COM/App'
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;


--create twitter weekly dataset
drop table if exists #dp_wkly_tw;
create table #dp_wkly_tw as
select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, 0 as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'Twitter' as platform,
'Twitter' as type
from 
#dim_dates b
left join 
(       
select monday_date,views,minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Twitter'
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;

--create instagram weekly dataset
drop table if exists #dp_wkly_ig;
create table #dp_wkly_ig as
select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, 0 as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'Instagram' as platform,
'Instagram' as type
from 
#dim_dates b
left join 
(       
select monday_date,views,minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Instagram'
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;

--create snapchat weekly dataset
drop table if exists #dp_wkly_sc;
create table #dp_wkly_sc as
select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, 0 as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'Snapchat' as platform,
'Snapchat' as type
from 
#dim_dates b
left join 
(       
select monday_date,views,minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Snapchat'
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;

--create youtube weekly dataset
drop table if exists #dp_wkly_yt;
create table #dp_wkly_yt as
select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, a.ad_impressions as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'Youtube' as platform,
a.type
from 
#dim_dates b
left join 
(       
select a.*,
case when a.type = 'Owned' then b.views else c.views end as views,
case when a.type = 'Owned' then b.hours_watched else c.hours_watched end as hours_watched
from
(
select 	date_trunc('week',view_date) as monday_date,type,
                sum(ad_impressions) as ad_impressions	 
                from fds_yt.agg_yt_monetization_summary
where view_date >= trunc(dateadd('year',-2,date_trunc('year',getdate()))) and view_date <= getdate()
group by 1,2
) a
left join
--Youtube-Owned
(
select 	 monday_date,
        'Owned' as type,
         views, 
	 minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Youtube-Owned'
) b
on a.monday_date = b.monday_date and a.type = b.type
left join
--Youtube-UGC
(
select 	 monday_date,
        'UGC' as type,
         views, 
	 minutes_watched/60 as hours_watched
from fds_cp.aggr_cp_weekly_consumption_by_platform 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Youtube-UGC'
) c
on a.monday_date = c.monday_date and a.type = c.type
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;


drop table if exists #dp_wkly;
create table #dp_wkly as 
select platform, type,cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_wk::decimal(15,1), hours_watched_tier2_wk::decimal(15,1), views_wk::decimal(15,1),
active_network_subscribers_wk::decimal(15,1), hours_per_tot_subscriber_wk::decimal(15,1), ad_impressions_wk::decimal(15,1),
network_subscriber_adds_wk::decimal(15,1),
new_adds_wk::decimal(15,1),
new_adds_direct_t3_wk::decimal(15,1),
reg_prospects_t2_to_t3_wk::decimal(15,1),
winback_adds_t2_to_t3_wk::decimal(15,1),
lp_adds_wk::decimal(15,1),
new_free_version_regns_wk::decimal(15,1),
network_losses_wk::decimal(15,1)
from (
select * from #dp_wkly_nw union all
select * from #dp_wkly_fb union all
select * from #dp_wkly_dc union all
select * from #dp_wkly_tw union all
select * from #dp_wkly_ig union all
select * from #dp_wkly_sc union all
select * from #dp_wkly_yt union all
select * from #dp_wkly_tt);


drop table if exists #dp_wkly1;
create table #dp_wkly1 as
select a.*, a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week,
b.cal_year as prev_cal_year, b.cal_year_week_num_mon as prev_cal_year_week_num_mon,
b.cal_year_mon_week_begin_date as prev_cal_year_mon_week_begin_date, b.cal_year_mon_week_end_date as prev_cal_year_mon_week_end_date,
coalesce(b.active_network_subscribers_wk,0) as prev_active_network_subscribers_wk, coalesce(b.hours_watched_wk,0) as prev_hours_watched_wk,
coalesce(b.hours_watched_tier2_wk,0) as prev_hours_watched_tier2_wk,
coalesce(b.hours_per_tot_subscriber_wk,0) as prev_hours_per_tot_subscriber_wk,coalesce(b.views_wk,0) as prev_views_wk,
coalesce(b.ad_impressions_wk,0) as prev_ad_impressions_wk,
coalesce(b.network_subscriber_adds_wk,0) as prev_network_subscriber_adds_wk,
coalesce(b.new_adds_wk,0) as prev_new_adds_wk,
coalesce(b.new_adds_direct_t3_wk,0) as prev_new_adds_direct_t3_wk,
coalesce(b.reg_prospects_t2_to_t3_wk,0) as prev_reg_prospects_t2_to_t3_wk,
coalesce(b.winback_adds_t2_to_t3_wk,0) as prev_winback_adds_t2_to_t3_wk,
coalesce(b.lp_adds_wk,0) as prev_lp_adds_wk,
coalesce(b.new_free_version_regns_wk,0) as prev_new_free_version_regns_wk,
coalesce(b.network_losses_wk,0) as prev_network_losses_wk
from 
#dp_wkly a
left join 
#dp_wkly b
on (a.cal_year-1) = b.cal_year and a.cal_year_week_num_mon = b.cal_year_week_num_mon and a.platform = b.platform and a.type=b.type;

--create monthly dataset
drop table if exists #dp_mthly;
create table #dp_mthly as
select a.platform,a.type, a.cal_year,a.cal_mth_num, a.cal_year_week_num_mon, a.cal_year_mon_week_begin_date, a.cal_year_mon_week_end_date,
a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week, 
a.prev_cal_year, a.prev_cal_year_week_num_mon,
a.prev_cal_year_mon_week_begin_date, a.prev_cal_year_mon_week_end_date,
sum(b.active_network_subscribers_wk) active_network_subscribers_mtd, sum(b.hours_watched_wk) hours_watched_mtd, sum(b.hours_watched_tier2_wk) hours_watched_tier2_mtd,
sum(b.hours_per_tot_subscriber_wk) hours_per_tot_subscriber_mtd, sum(b.views_wk) views_mtd, sum(b.ad_impressions_wk) ad_impressions_mtd,
sum(b.network_subscriber_adds_wk) as network_subscriber_adds_mtd,
sum(b.new_adds_wk) as new_adds_mtd,
sum(b.new_adds_direct_t3_wk) as new_adds_direct_t3_mtd,
sum(b.reg_prospects_t2_to_t3_wk) as reg_prospects_t2_to_t3_mtd,
sum(b.winback_adds_t2_to_t3_wk) as winback_adds_t2_to_t3_mtd,
sum(b.lp_adds_wk) as lp_adds_mtd,
sum(b.new_free_version_regns_wk) as new_free_version_regns_mtd,
sum(b.network_losses_wk) as network_losses_mtd,
sum(b.prev_active_network_subscribers_wk) prev_active_network_subscribers_mtd, sum(b.prev_hours_watched_wk) prev_hours_watched_mtd, 
sum(b.prev_hours_watched_tier2_wk) prev_hours_watched_tier2_mtd,
sum(b.prev_hours_per_tot_subscriber_wk) prev_hours_per_tot_subscriber_mtd, 
sum(b.prev_views_wk) prev_views_mtd, sum(b.prev_ad_impressions_wk) prev_ad_impressions_mtd,
sum(b.prev_network_subscriber_adds_wk) as prev_network_subscriber_adds_mtd,
sum(b.prev_new_adds_wk) as prev_new_adds_mtd,
sum(b.prev_new_adds_direct_t3_wk) as prev_new_adds_direct_t3_mtd,
sum(b.prev_reg_prospects_t2_to_t3_wk) as prev_reg_prospects_t2_to_t3_mtd,
sum(b.prev_winback_adds_t2_to_t3_wk) as prev_winback_adds_t2_to_t3_mtd,
sum(b.prev_lp_adds_wk) as prev_lp_adds_mtd,
sum(b.prev_new_free_version_regns_wk) as prev_new_free_version_regns_mtd,
sum(b.prev_network_losses_wk) as prev_network_losses_mtd
from #dp_wkly1 a
left join #dp_wkly1 b
on a.cal_year = b.cal_year and a.cal_mth_num = b.cal_mth_num and a.cal_year_week_num_mon >= b.cal_year_week_num_mon
 and a.platform = b.platform and a.type=b.type
group by 1,2,3,4,5,6,7,8,9,10,11,12;

--create yearly dataset
drop table if exists #dp_yrly;
create table #dp_yrly as
select a.platform,a.type,a.cal_year,a.cal_mth_num, a.cal_year_week_num_mon, a.cal_year_mon_week_begin_date, a.cal_year_mon_week_end_date,
a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week,
a.prev_cal_year, a.prev_cal_year_week_num_mon,
a.prev_cal_year_mon_week_begin_date, a.prev_cal_year_mon_week_end_date,
sum(b.active_network_subscribers_wk) active_network_subscribers_ytd, 
sum(b.hours_watched_wk) hours_watched_ytd, 
sum(b.hours_watched_tier2_wk) hours_watched_tier2_ytd,
sum(b.hours_per_tot_subscriber_wk) hours_per_tot_subscriber_ytd, 
sum(b.views_wk) views_ytd, sum(b.ad_impressions_wk) ad_impressions_ytd,
sum(b.network_subscriber_adds_wk) as network_subscriber_adds_ytd,
sum(b.new_adds_wk) as new_adds_ytd,
sum(b.new_adds_direct_t3_wk) as new_adds_direct_t3_ytd,
sum(b.reg_prospects_t2_to_t3_wk) as reg_prospects_t2_to_t3_ytd,
sum(b.winback_adds_t2_to_t3_wk) as winback_adds_t2_to_t3_ytd,
sum(b.lp_adds_wk) as lp_adds_ytd,
sum(b.new_free_version_regns_wk) as new_free_version_regns_ytd,
sum(b.network_losses_wk) as network_losses_ytd,
sum(b.prev_active_network_subscribers_wk) prev_active_network_subscribers_ytd, 
sum(b.prev_hours_watched_wk) prev_hours_watched_ytd, 
sum(b.prev_hours_watched_tier2_wk) prev_hours_watched_tier2_ytd,
sum(b.prev_hours_per_tot_subscriber_wk) prev_hours_per_tot_subscriber_ytd, 
sum(b.prev_views_wk) prev_views_ytd, sum(b.prev_ad_impressions_wk) prev_ad_impressions_ytd,
sum(b.prev_network_subscriber_adds_wk) as prev_network_subscriber_adds_ytd,
sum(b.prev_new_adds_wk) as prev_new_adds_ytd,
sum(b.prev_new_adds_direct_t3_wk) as prev_new_adds_direct_t3_ytd,
sum(b.prev_reg_prospects_t2_to_t3_wk) as prev_reg_prospects_t2_to_t3_ytd,
sum(b.prev_winback_adds_t2_to_t3_wk) as prev_winback_adds_t2_to_t3_ytd,
sum(b.prev_lp_adds_wk) as prev_lp_adds_ytd,
sum(b.prev_new_free_version_regns_wk) as prev_new_free_version_regns_ytd,
sum(b.prev_network_losses_wk) as prev_network_losses_ytd
from #dp_wkly1 a
left join #dp_wkly1 b
on a.cal_year = b.cal_year and a.cal_year_week_num_mon >= b.cal_year_week_num_mon and a.platform = b.platform and a.type=b.type
group by 1,2,3,4,5,6,7,8,9,10,11,12;


--create yearly dataset for hours per tot subs
drop table if exists #dp_yrly1;
create table #dp_yrly1 as
select a.platform, a.type, a.cal_year, a.cal_mth_num, a.cal_year_week_num_mon, a.cal_year_mon_week_begin_date, a.cal_year_mon_week_end_date, 
a.week, a.prev_cal_year, a.prev_cal_year_week_num_mon, a.prev_cal_year_mon_week_begin_date, a.prev_cal_year_mon_week_end_date,
b.active_network_subscribers_wk as active_network_subscribers_ytd, a.hours_watched_ytd, a.hours_watched_tier2_ytd, 
a.hours_watched_ytd/nullif(b.active_network_subscribers_wk,0) as hours_per_tot_subscriber_ytd, 
a.views_ytd, a.ad_impressions_ytd, a.network_subscriber_adds_ytd, a.new_adds_ytd, a.new_adds_direct_t3_ytd, a.reg_prospects_t2_to_t3_ytd,
a.winback_adds_t2_to_t3_ytd, a.lp_adds_ytd, a.new_free_version_regns_ytd, a.network_losses_ytd, 
b.prev_active_network_subscribers_wk as prev_active_network_subscribers_ytd, a.prev_hours_watched_ytd, a.prev_hours_watched_tier2_ytd,
a.prev_hours_watched_ytd/nullif(b.prev_active_network_subscribers_wk,0) as prev_hours_per_tot_subscriber_ytd, 
a.prev_views_ytd, a.prev_ad_impressions_ytd, a.prev_network_subscriber_adds_ytd, a.prev_new_adds_ytd, a.prev_new_adds_direct_t3_ytd, a.prev_reg_prospects_t2_to_t3_ytd,
a.prev_winback_adds_t2_to_t3_ytd, a.prev_lp_adds_ytd, a.prev_new_free_version_regns_ytd, a.prev_network_losses_ytd
from #dp_yrly a
left join #dp_wkly1 b
on a.platform = b.platform
and a.type = b.type
and a.cal_year = b.cal_year
and a.cal_mth_num = b.cal_mth_num
and a.cal_year_week_num_mon = b.cal_year_week_num_mon
and a.cal_year_mon_week_begin_date = b.cal_year_mon_week_begin_date
and a.cal_year_mon_week_end_date = b.cal_year_mon_week_end_date
and a.week = b.week
where a.platform = 'Network' and b.platform = 'Network'
;

--pivot weekly dataset
drop table if exists #dp_wkly_pivot;
create table #dp_wkly_pivot as
select * from
(
select 'Weekly' as granularity, platform, type, 'Hours Watched' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, type, 'Hours Watched Tier2' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_tier2_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_tier2_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, case when platform ='Network' then 'Active Viewers' else 'Views' end as Metric, 
week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, views_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_views_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, type, 'Ad Impressions' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, ad_impressions_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_ad_impressions_wk as prev_year_value
from #dp_wkly1  where platform = 'Youtube'
union all
select 'Weekly' as granularity, platform, type, 'Active Network Subscribers' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, active_network_subscribers_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_active_network_subscribers_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'Hours per total Subscriber' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_per_tot_subscriber_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_per_tot_subscriber_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'Network Subscriber Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_subscriber_adds_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_subscriber_adds_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'New Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'New Adds (Direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_direct_t3_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_direct_t3_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'Registered Prospects (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, reg_prospects_t2_to_t3_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_reg_prospects_t2_to_t3_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'Winback Adds (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, winback_adds_t2_to_t3_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_winback_adds_t2_to_t3_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'License Partner Adds (direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, lp_adds_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_lp_adds_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'New Free Version Registrations (New to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_free_version_regns_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_free_version_regns_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'Weekly' as granularity, platform, type, 'Network Losses (T3 to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_losses_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_losses_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
);

--pivot monthly dataset
drop table if exists #dp_mthly_pivot;
create table #dp_mthly_pivot as
select * from
(
select 'MTD' as granularity, platform, type, 'Hours Watched' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, type, 'Hours Watched Tier2' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_tier2_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_tier2_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, case when platform ='Network' then 'Active Viewers' else 'Views' end as Metric, 
week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, views_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_views_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, type, 'Ad Impressions' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, ad_impressions_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_ad_impressions_mtd as prev_year_value
from #dp_mthly  where platform = 'Youtube'
union all
select 'MTD' as granularity, platform, type, 'Active Network Subscribers' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, active_network_subscribers_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_active_network_subscribers_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Hours per total Subscriber' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_per_tot_subscriber_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_per_tot_subscriber_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Network Subscriber Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_subscriber_adds_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_subscriber_adds_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'New Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'New Adds (Direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_direct_t3_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_direct_t3_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Registered Prospects (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, reg_prospects_t2_to_t3_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_reg_prospects_t2_to_t3_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Winback Adds (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, winback_adds_t2_to_t3_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_winback_adds_t2_to_t3_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'License Partner Adds (direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, lp_adds_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_lp_adds_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'New Free Version Registrations (New to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_free_version_regns_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_free_version_regns_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Network Losses (T3 to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_losses_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_losses_mtd as prev_year_value
from #dp_mthly where platform = 'Network'
);

--pivot yearly dataset
drop table if exists #dp_yrly_pivot;
create table #dp_yrly_pivot as
select * from
(
select 'YTD' as granularity, platform, type, 'Hours Watched' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, type, 'Hours Watched Tier2' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_tier2_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_tier2_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, case when platform ='Network' then 'Active Viewers' else 'Views' end as Metric, 
week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, views_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_views_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, type, 'Ad Impressions' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, ad_impressions_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_ad_impressions_ytd as prev_year_value
from #dp_yrly where platform = 'Youtube'
union all
select 'YTD' as granularity, platform, type, 'Active Network Subscribers' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, active_network_subscribers_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_active_network_subscribers_wk as prev_year_value
from #dp_wkly1 where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'Hours per total Subscriber' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_per_tot_subscriber_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_per_tot_subscriber_ytd as prev_year_value
from #dp_yrly1 where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'Network Subscriber Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_subscriber_adds_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_subscriber_adds_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'New Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'New Adds (Direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_direct_t3_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_direct_t3_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'Registered Prospects (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, reg_prospects_t2_to_t3_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_reg_prospects_t2_to_t3_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'Winback Adds (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, winback_adds_t2_to_t3_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_winback_adds_t2_to_t3_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'License Partner Adds (direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, lp_adds_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_lp_adds_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'New Free Version Registrations (New to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_free_version_regns_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_free_version_regns_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
union all
select 'YTD' as granularity, platform, type, 'Network Losses (T3 to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_losses_ytd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_losses_ytd as prev_year_value
from #dp_yrly where platform = 'Network'
);

drop table if exists #consolidation;
create table #consolidation as
select 
a.granularity, a.platform, a.type, a.metric, a.cal_year as year,
a.cal_mth_num as month, a.cal_year_week_num_mon as week, a.cal_year_mon_week_begin_date as start_date,
a.cal_year_mon_week_end_date as end_date, a.value, a.prev_cal_year as prev_year,
a.prev_cal_year_week_num_mon as prev_year_week, a.prev_cal_year_mon_week_begin_date as prev_year_start_date,
a.prev_cal_year_mon_week_end_date as prev_year_end_date,a.prev_year_value
from 
(select * from #dp_wkly_pivot union all
 select * from #dp_mthly_pivot union all
 select * from #dp_yrly_pivot) a;
 
drop table if exists #final;
create table #final as
select granularity, platform, type, metric, a.year, a.month, week, 
case when granularity = 'MTD' then b.start_date 
     when granularity = 'YTD' then c.start_date else a.start_date end as start_date,
end_date, value, prev_year, prev_year_week, 
case when granularity = 'MTD' then b.prev_year_start_date 
     when granularity = 'YTD' then c.prev_year_start_date else a.prev_year_start_date end as prev_year_start_date,     
prev_year_end_date, prev_year_value
from #consolidation a
left join
(select year,month, min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from #consolidation group by 1,2) b
on a.year = b.year
and a.month = b.month
left join
(select year,min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from #consolidation group by 1 ) c
on a.year = c.year
;

drop table if exists #final1;
create table #final1 as
select a.granularity, a.platform, a.type, a.metric, a.year, a.month, a.week, a.start_date, a.end_date,
case when a.granularity = 'MTD' and a.platform = 'Network' and a.metric = 'Active Viewers' then b.cntd_views_mtd
     when a.granularity = 'YTD' and a.platform = 'Network' and a.metric = 'Active Viewers' then d.cntd_views_ytd
else a.value end  as value,
a.prev_year, a.prev_year_week, a.prev_year_start_date, a.prev_year_end_date,
case when a.granularity = 'MTD' and a.platform = 'Network' and a.metric = 'Active Viewers' then c.prev_cntd_views_mtd
     when a.granularity = 'YTD' and a.platform = 'Network' and a.metric = 'Active Viewers' then e.prev_cntd_views_ytd
else a.prev_year_value end as prev_year_value
from #final a 
left join 
(
select a.granularity, a.platform, a.type, a.metric, a.year, a.month, a.week, a.start_date, a.end_date,
 count(distinct b.src_fan_id) as cntd_views_mtd
from #final a
left join fds_nplus.fact_daily_content_viewership b
on trunc(b.stream_start_dttm) between a.start_date and a.end_date
where b.subs_tier = '2' 
and a.platform = 'Network'  
and a.metric = 'Active Viewers' and a.granularity = 'MTD'
group by 1,2,3,4,5,6,7,8,9
) b
on a.granularity = b.granularity
and a.platform = b.platform
and a.type = b.type
and a.metric = b.metric
and a.year = b.year
and a.month = b.month
and a.week = b.week
and a.start_date = b.start_date
and a.end_date = b.end_date
left join 
(
select a.granularity, a.platform, a.type, a.metric, a.year, a.month, a.week, a.start_date, a.end_date,
 count(distinct b.src_fan_id) as prev_cntd_views_mtd
from #final a
left join fds_nplus.fact_daily_content_viewership b
on trunc(b.stream_start_dttm) between a.prev_year_start_date and a.prev_year_end_date
where b.subs_tier = '2' 
and a.platform = 'Network'  
and a.metric = 'Active Viewers' and a.granularity = 'MTD'
group by 1,2,3,4,5,6,7,8,9
) c
on a.granularity = c.granularity
and a.platform = c.platform
and a.type = c.type
and a.metric = c.metric
and a.year = c.year
and a.month = c.month
and a.week = c.week
and a.start_date = c.start_date
and a.end_date = c.end_date
left join 
(
select a.granularity, a.platform, a.type, a.metric, a.year, a.month, a.week, a.start_date, a.end_date,
 count(distinct b.src_fan_id) as cntd_views_ytd
from #final a
left join fds_nplus.fact_daily_content_viewership b
on trunc(b.stream_start_dttm) between a.start_date and a.end_date
where b.subs_tier = '2' and trunc(b.stream_start_dttm) >= '2020-06-01'
and a.platform = 'Network'  
and a.metric = 'Active Viewers' and a.granularity = 'YTD'
group by 1,2,3,4,5,6,7,8,9
) d
on a.granularity = d.granularity
and a.platform = d.platform
and a.type = d.type
and a.metric = d.metric
and a.year = d.year
and a.month = d.month
and a.week = d.week
and a.start_date = d.start_date
and a.end_date = d.end_date
left join 
(
select a.granularity, a.platform, a.type, a.metric, a.year, a.month, a.week, a.start_date, a.end_date,
 count(distinct b.src_fan_id) as prev_cntd_views_ytd
from #final a
left join fds_nplus.fact_daily_content_viewership b
on trunc(b.stream_start_dttm) between a.prev_year_start_date and a.prev_year_end_date
where b.subs_tier = '2' and trunc(b.stream_start_dttm) >= '2020-06-01'
and a.platform = 'Network'  
and a.metric = 'Active Viewers' and a.granularity = 'YTD'
group by 1,2,3,4,5,6,7,8,9
) e
on a.granularity = e.granularity
and a.platform = e.platform
and a.type = e.type
and a.metric = e.metric
and a.year = e.year
and a.month = e.month
and a.week = e.week
and a.start_date = e.start_date
and a.end_date = e.end_date;
"]
	})}}
select *,'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', sysdate),'YYYY_MM_DD_HH_MI_SS')+'_CP' etl_batch_id, 
'bi_dbt_user_prd' AS etl_insert_user_id,
convert_timezone('AMERICA/NEW_YORK', sysdate) AS etl_insert_rec_dttm,
cast (NULL as varchar) AS etl_update_user_id,
CAST( NULL AS TIMESTAMP) AS etl_update_rec_dttm from #final1 order by platform, granularity, metric, year, week