

  create view "entdwdb"."fds_nl"."vw_aggr_nl_monthly_primetime_ratings__dbt_tmp" as (
    

--Primetime program ratings for non WWE program airing days(monthly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_monthly_primetime_ratings
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : Primetime Rating Monthly Aggregate View consist of rating details of all channels and programs to be rolled up from Timeperiod 	 Viewership Ratings table for Tuesday, Thursday, Saturday and Sunday on monthly-basis.
*************************************************************************************************************************************************
*/

SELECT b.mth_abbr_nm as rpt_month_nm,
       b.cal_year as rpt_year,
       c.broadcast_network_name as broadcast_network_name, 
       d.src_daypart_name as src_daypart_name, 
       src_playback_period_cd,
       src_demographic_group,
       avg(avg_audience_proj_000) as avg_audience_proj_000, 
       avg(avg_audience_pct) as avg_audience_pct, 
       avg(avg_pct_nw_cvg_area) as avg_pct_nw_cvg_area
      --sum(avg_viewing_hours_units) as avg_viewing_hours_units
FROM "entdwdb"."fds_nl"."fact_nl_timeperiod_viewership_ratings" a
JOIN (SELECT dim_date_id, mth_abbr_nm, cal_year_qtr_desc, cal_year 
       FROM "entdwdb"."cdm"."dim_date"
       WHERE day_of_week_abbr_nm IN ('tue','thu','sat','sun')
     ) b
ON a.rpt_startdate_id = b.dim_date_id
LEFT JOIN "entdwdb"."fds_nl"."dim_nl_broadcast_network"  c ON a.dim_nl_broadcast_network_id = c.dim_nl_broadcast_network_id
LEFT JOIN  "entdwdb"."fds_nl"."dim_nl_daypart"   d ON a.dim_nl_daypart_id = d.dim_nl_daypart_id
GROUP BY 1,2,3,4,5,6
  ) ;
