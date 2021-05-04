{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'monthly_talent_scorecard',
		 "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": 'grant select on {{this}} to public'
        })
}}

select date,
case when entity_type like 'Character' then lineage_name when entity_type='Group' then character_lineage_name end as lineage_name,
case when entity_type like 'Character' then lineage_wweid when entity_type='Group' then character_lineage_wweid end as lineage_wweid,
achievement
from (select date,lineage_name,lineage_wweid,entity_type,achievement
from {{source('fds_cpg', 'rpt_daily_talent_equity_centralized')}}
where lower(achievement) like '%champion%'
group by 1,2,3,4,5) a
left join {{source('fds_mdm', 'characterlineage_grouplineage')}} q on a.lineage_name=q.group_lineage_name group by 1,2,3,4


