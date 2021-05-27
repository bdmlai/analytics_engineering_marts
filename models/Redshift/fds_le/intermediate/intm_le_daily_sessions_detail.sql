{{
  config({
		"materialized": 'ephemeral'
  })
}}
select 
	a.date, 
	a.uid_hit,
	b.full_visitor_id::varchar(250),
	b.visit_id,
	b.total_time_on_site,
	b.trafficsource_campaign::varchar(250),
	b.device_devicecategory::varchar(250),
	b.geonetwork_country::varchar(250),
	b.as_on_date as as_on_date_ga
from
	(select 
		date,
		uid_hit,
		visit_id_calc
	 from 
		{{ref('intm_le_daily_hit_sessions')}}
	 where rank_session = 1) a
left join
	{{source('hive_udl_le','ga_le_daily_sessions')}} b
on
	a.date = b.date and 
	coalesce(a.visit_id_calc, 'n/a') = b.visit_id_calc