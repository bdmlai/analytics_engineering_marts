{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, 
	case when lower(title) like '% smackdown %' then 'SmackDown'
		when lower(title) like '% smackdown!' then 'SmackDown'
		when lower(title) like '% smackdown: %' then 'SmackDown'
		when lower(title) like '% smackdown' then 'SmackDown'
		when lower(title) like 'smackdown %' then 'SmackDown'
		when lower(title) like '% :smackdown %' then 'SmackDown'
		when lower(title) like '% smackdown, %' then 'SmackDown'
		when lower(title) like '% sd live %' then 'SmackDown'
		when lower(title) like '% sd live, %' then 'SmackDown'
		when lower(title) like '% raw %' then 'Raw'
		when lower(title) like '% raw! %' then 'Raw'
		when lower(title) like '% raw!' then 'Raw'
		when lower(title) like '% raw: %' then 'Raw'
		when lower(title) like '% raw' then 'Raw'
		when lower(title) like 'raw %' then 'Raw'
		when lower(title) like '% :raw %' then 'Raw'
		when lower(title) like '% raw,%' then 'Raw' 
		when lower(title) like '% nxt %' then 'NXT'
		when lower(title) like '% nxt: %' then 'NXT'
		when lower(title) like '% nxt' then 'NXT'
		when lower(title) like 'nxt %' then 'NXT'
		when lower(title) like '% :nxt %' then 'NXT' 
		when lower(title) like '%nxt,%' then 'NXT' 
		when lower(title) like '%nxt!%' then 'NXT'
		when lower(title) like '%nxt!' then 'NXT'
	else 'other programs' end as series_name
from {{ref('intm_fbk_amg_cnt_grp_2')}}