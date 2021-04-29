{{
  config({
		'schema': 'fds_fbk',"tags": 'fbk_amg_content_group',
		"materialized": 'table', "persist_docs": {'relation' : true, 'columns' : true},
		'post-hook': 'grant select on {{this}} to public'		
  })
}}
{% set descript = [("kickoff","Kickoff"),("pre-show","Kickoff"),("red carpet","Kickoff"),("backlash","PPV Clip"),("battlegrounds","PPV Clip"),("battleground","PPV Clip"),("clash of champions","PPV Clip"),("elimination chamber","PPV Clip"),("extreme rules","PPV Clip"), ("fastlane","PPV Clip"),("royal rumble","PPV Clip"),("hell in a cell","PPV Clip"),("money in the bank","PPV Clip"),("no mercy","PPV Clip"),(" wwe payback ","PPV Clip"),(" wwe payback: ","PPV Clip"),("stomping grounds","PPV Clip"),("summerslam","PPV Clip"),("super showdown","PPV Clip"),("survivor series","PPV Clip"),(" wwe tlc ","PPV Clip"),("wrestlemania","PPV Clip"),(" crown jewel ","PPV Clip"),(" crown jewel: ","PPV Clip")] %}

select *, duration*1.00/60 as min_duration,
    case   
		when channel_name='WWE Performance Center' then 'WWE Performance Center'
		when channel_name in ('Total Bellas (WWE)', 'Nikki Bella', 'Brie Bella') then 'The Bella Twins'
		when channel_name='Total Divas' then 'Total Divas'
		when channel_name='UUDD' then 'UpUpDownDown'
		when series_name='Raw' and lower(title) like '%full episode%' then 'Full Episode'
		when series_name='SmackDown' and lower(title) like '%full episode%' then 'Full Episode'
		when title like '%WWE Raw%' and lower(title) like '%full episode%' then 'Full Episode'
		when title like '%WWE SmackDown%' and lower(title) like '%full episode%' then 'Full Episode'
		when lower(title) like '%full match%' then 'Full Match'
		when (lower(title) like '%(full%' and lower(title) like '%match)%') then 'Full Match'
	{% for schedule_name,j in descript %}
	 when lower(title) like '%{{schedule_name}}%' then '{{j}}'
	{% endfor %}
		when lower(title) like '% wwe payback!' then 'PPV Clip'
		when lower(title) like 'wwe payback %' then 'PPV Clip'
		when lower(title) like 'wwe payback: %' then 'PPV Clip'
		when lower(title) like 'wwe tlc %' then 'PPV Clip'
		when lower(title) like 'wwe tlc: %' then 'PPV Clip'
		when lower(title) like '%wwe evolution %' then 'PPV Clip'
		when lower(title) like '%wwe evolution' then 'PPV Clip'
		when lower(title) like 'wwe evolution %' then 'PPV Clip'
		when lower(title) like 'wwe evolution: %' then 'PPV Clip'
		when series_name in ('Raw') then 'Raw'
		when series_name in ('NXT') then 'NXT'
		when series_name in ('SmackDown') then 'SmackDown'
	else 'Misc' end as amg_content_group
from  {{ref('intm_fbk_amg_cnt_grp_3')}}