
{{
  config({
		"materialized": 'ephemeral'
  })
}}

select  src_market_break,src_demographic_group,broadcast_date,mxm_source,
case when upper(src_broadcast_network_name)= 'FOX' then 'FOX Affiliates' 
when upper(src_broadcast_network_name) = 'TURNER NETWORK TELEVISION' then 'TNT'
WHEN  upper(src_broadcast_network_name) = 'USA NETWORK' THEN 'USA' END AS src_broadcast_network_name,
((split_part(program_telecast_rpt_starttime, ':', 1) :: int *60*60 +
 split_part(program_telecast_rpt_starttime, ':', 2) :: int*60 +
  (split_part(program_telecast_rpt_starttime, ':', 3) :: int  ))
 + (min_of_pgm_value - 1)*60) AS TIME_MINUTE ,
most_current_us_audience_avg_proj_000
FROM  {{source('fds_nl','fact_nl_minxmin_ratings')}} 
where src_playback_period_cd in ('Live | TV with Digital | Linear with VOD')


