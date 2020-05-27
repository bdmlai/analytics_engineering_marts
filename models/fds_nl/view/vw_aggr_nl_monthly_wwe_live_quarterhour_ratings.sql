{{
  config({
	"materialized": 'view',
	"post-hook": ["COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_quarterhour_ratings.broadcast_month IS 'Show broadcast month'"]
	})
}}

--QH ratings for WWE program (monthly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_monthly_wwe_live_quarterhour_ratings
   Schema	   : fds_nl
   Contributor : Sudhakar Andugula
   Description : WWE Live Quarter Hour Rating Monthly Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from Quarter Hour Viewership Ratings Table on monthly-basis
**************************************************************************************************************************************************
*/
select b.mth_abbr_nm as broadcast_month, b.cal_year as broadcast_year, src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id, interval_starttime, interval_endtime,
--Duration Weighted Averages are taking for avg_audience_proj_000, avg_audience_pct and avg_pct_nw_cvg_area here..
(sum(avg_audience_proj_000 * interval_duration)/sum(nvl2(avg_audience_proj_000, interval_duration, null))) as avg_audience_proj_000,
(sum(avg_audience_pct * interval_duration)/sum(nvl2(avg_audience_pct, interval_duration, null))) as avg_audience_pct,
(sum(avg_pct_nw_cvg_area * interval_duration)/sum(nvl2(avg_pct_nw_cvg_area, interval_duration, null))) as avg_pct_nw_cvg_area, 
sum(avg_viewing_hours_units) as avg_viewing_hours_units
from (select broadcast_date_id, src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id, interval_starttime, interval_endtime, interval_duration, avg_viewing_hours_units, avg_audience_proj_000, avg_audience_pct, avg_pct_nw_cvg_area
from {{ref('fact_nl_quaterhour_viewership_ratings')}}
where (src_broadcast_network_id, src_program_id) in ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131))) a
left join {{ref('dim_date')}} b on a.broadcast_date_id = b.dim_date_id
group by 1,2,3,4,5,6,7,8