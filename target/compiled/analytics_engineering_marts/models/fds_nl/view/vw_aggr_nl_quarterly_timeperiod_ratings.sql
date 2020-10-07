--Timeperiod program ratings (quarterly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_quarterly_timeperiod_ratings
   Schema	   : fds_nl
   Contributor : Remya K Nair
   Description : Timeperiod Rating Quarterly Aggregate View consist of rating details of all channels and programs to be rolled up from Timeperiod 	 Viewership Ratings table on quarterly-basis.
*************************************************************************************************************************************************
*/
 

SELECT  substring(b.cal_year_qtr_desc, 5, 2) as rpt_quarter_nm,
        b.cal_year    as rpt_year, 
        c.broadcast_network_name as broadcast_network_name,
        d.src_daypart_name as src_daypart_name,
        src_playback_period_cd,
        src_demographic_group,
        avg(avg_audience_proj_000) as avg_audience_proj_000,
        avg(avg_audience_pct) as avg_audience_pct,
        avg(avg_pct_nw_cvg_area) as avg_pct_nw_cvg_area
        --sum(avg_viewing_hours_units) as avg_viewing_hours_units
FROM       "entdwdb"."fds_nl"."fact_nl_timeperiod_viewership_ratings" a
LEFT JOIN  "entdwdb"."cdm"."dim_date"   b on a.rpt_startdate_id = b.dim_date_id
LEFT JOIN  "entdwdb"."fds_nl"."dim_nl_broadcast_network"   c on a.dim_nl_broadcast_network_id = c.dim_nl_broadcast_network_id
LEFT JOIN  "entdwdb"."fds_nl"."dim_nl_daypart"   d on a.dim_nl_daypart_id = d.dim_nl_daypart_id
GROUP BY  1,2,3,4,5,6