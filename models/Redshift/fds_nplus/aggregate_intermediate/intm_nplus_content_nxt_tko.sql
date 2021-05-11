{{
  config({
		"materialized": 'ephemeral'
  })
}}
select distinct premiere_date, network_id, episode_nm as title from {{source('cdm','dim_content_classification_title')}}
        where series_group = 'NXT TakeOver'
		and premiere_date > (select max(premiere_date) from fds_nplus.rpt_nplus_daily_nxt_tko_streams)
        and right(episode_nm,4) != '(ES)' 
        and production_id not like '%span_ntwk%'