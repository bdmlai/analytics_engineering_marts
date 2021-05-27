{{
  config({
		"materialized": 'ephemeral'
  })
}}
select 
	a.date,
	a.uid_hit,
	b.visit_id_calc,
	row_number() over(partition by a.date, a.uid_hit order by b.total_time_on_site desc nulls last, b.visit_id) as rank_session
from
	(select distinct
		date,
		case
			when len(split_part(trim(hit_page_page_path), 'uid=', 2)) > 36
			then substring((split_part(trim(hit_page_page_path), 'uid=', 2)), 1, 36)
			else split_part(trim(hit_page_page_path), 'uid=', 2)
		end as uid_hit,
		visit_id_calc
	from
		{{source('hive_udl_le','ga_le_daily_hits')}}
	where
		hit_page_page_path like '%uid=%') a
left join
	{{source('hive_udl_le','ga_le_daily_sessions')}} b
on
	a.date = b.date and 
	a.visit_id_calc = b.visit_id_calc