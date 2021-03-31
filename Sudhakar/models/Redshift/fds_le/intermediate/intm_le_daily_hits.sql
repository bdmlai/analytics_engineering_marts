{{
  config({
		"materialized": 'ephemeral'
  })
}}
select distinct
    date,
    case
        when len(split_part(trim(hit_page_page_path), 'uid=', 2)) > 36
        then substring((split_part(trim(hit_page_page_path), 'uid=', 2)), 1, 36)
        else split_part(trim(hit_page_page_path), 'uid=', 2)
    end as uid_hit
from
    {{source('hive_udl_le','ga_le_daily_hits')}}
where
    hit_page_page_path like '%uid=%' 