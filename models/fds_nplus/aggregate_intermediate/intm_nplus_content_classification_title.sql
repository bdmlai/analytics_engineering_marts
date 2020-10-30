{{
  config({
		"materialized": 'ephemeral'
  })
}}

select distinct premiere_date, network_id, episode_nm as title from {{source('cdm','dim_content_classification_title')}}
 where series_group = 'WWE PPV' and premiere_date >= current_date-7 and right(episode_nm,4) != '(ES)' 
   and network_id like '%ppv%'
