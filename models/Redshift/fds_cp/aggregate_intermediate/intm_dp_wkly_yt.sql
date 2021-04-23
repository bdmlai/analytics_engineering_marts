{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

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
'Youtube'::varchar(12) as platform,
a.type
from 
{{ref('intm_dim_dates')}} b
left join 
(       
select a.*,
case when a.type = 'Owned' then b.views else c.views end as views,
case when a.type = 'Owned' then b.hours_watched else c.hours_watched end as hours_watched
from
(
select 	date_trunc('week',view_date) as monday_date,type,
                sum(ad_impressions) as ad_impressions	 
                from {{source('fds_yt','agg_yt_monetization_summary')}}
where view_date >= trunc(dateadd('year',-2,date_trunc('year',getdate()))) and view_date <= getdate()
group by 1,2
) a
left join
--Youtube-Owned
(
select 	 monday_date,
        'Owned'::varchar(12) as type,
         views, 
	 minutes_watched/60 as hours_watched
from {{source('fds_cp','vw_aggr_cp_weekly_consumption_by_platform')}} 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Youtube-Owned'
) b
on a.monday_date = b.monday_date and a.type = b.type
left join
--Youtube-UGC
(
select 	 monday_date,
        'UGC'::varchar(12) as type,
         views, 
	 minutes_watched/60 as hours_watched
from {{source('fds_cp','vw_aggr_cp_weekly_consumption_by_platform')}} 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Youtube-UGC'
) c
on a.monday_date = c.monday_date and a.type = c.type
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date