
{{
  config({
		'schema': 'fds_nl',
		"materialized": 'ephemeral'
  })
}}



 select a.broadcast_Date,a.src_broadcast_network_name,a.src_demographic_group,
 a.time_minute,
 dense_rank() over(partition by src_broadcast_network_name,broadcast_Date,src_demographic_group  order by 
switch_percent desc NULLS LAST)
as switch_percent_rank
from {{ref('intermediate_nl_absolute_switch_stay_detail')}}  a
where ((lower(a.source_name) in ('nxt','smackdown') and time_minute  between '20:05:00'
and '21:54:00') or (lower(a.source_name) in ('raw') and time_minute  between '20:05:00'
and '22:54:00')  or 
( lower(a.source_name)  in ('aew'))) and  comment is null