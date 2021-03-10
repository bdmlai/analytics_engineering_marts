--Timeperiod program ratings (yearly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_yearly_timeperiod_ratings
   Schema	   : fds_nl
   Contributor : Remya K Nair
   Description : Timeperiod Rating Yearly Aggregate View consist of rating details of all channels and programs to be rolled up from Timeperiod 	 Viewership Ratings table on yearly-basis.
*************************************************************************************************************************************************
*/
 {{
  config({
	'schema': 'fds_nl',
	"materialized": 'view',
	"post-hook": ["COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.rpt_year IS 'Reporting year';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.broadcast_network_name IS 'Broadcast netowrk Name or the channel name or view source name';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.src_daypart_name IS 'A unique character identifier for an individual daypart description ';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.src_playback_period_cd IS 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.';  
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.src_demographic_group IS 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).';  
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.avg_audience_proj_000 IS 'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.avg_audience_pct IS 'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_yearly_timeperiod_ratings.avg_pct_nw_cvg_area IS 'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)';  
				  "]
	})
}}
SELECT  b.cal_year    as rpt_year, 
        c.broadcast_network_name as broadcast_network_name,
        d.src_daypart_name as src_daypart_name,
        src_playback_period_cd,
        src_demographic_group,
        avg(avg_audience_proj_000) as avg_audience_proj_000,
        avg(avg_audience_pct) as avg_audience_pct,
        avg(avg_pct_nw_cvg_area) as avg_pct_nw_cvg_area
        --sum(avg_viewing_hours_units) as avg_viewing_hours_units       
FROM       {{source('fds_nl','fact_nl_timeperiod_viewership_ratings')}} a
LEFT JOIN  {{source('cdm','dim_date')}}  b on a.rpt_startdate_id = b.dim_date_id
LEFT JOIN  {{source('fds_nl','dim_nl_broadcast_network')}}  c on a.dim_nl_broadcast_network_id = c.dim_nl_broadcast_network_id
LEFT JOIN  {{source('fds_nl','dim_nl_daypart')}}   d on a.dim_nl_daypart_id = d.dim_nl_daypart_id
GROUP BY  1,2,3,4,5