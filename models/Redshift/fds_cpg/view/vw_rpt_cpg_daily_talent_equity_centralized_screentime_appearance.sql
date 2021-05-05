{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'monthly_talent_scorecard',
		 "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": 'grant select on {{this}} to public'
        })
}}

SELECT  date 
       ,CASE WHEN entity_type like 'Character' THEN lineage_name 
             WHEN entity_type='Group' THEN character_lineage_name END  AS lineage_name 
       ,CASE WHEN entity_type like 'Character' THEN lineage_wweid 
             WHEN entity_type='Group' THEN character_lineage_wweid END AS lineage_wweid 
       ,SUM(duration)                                                  AS screentime
FROM 
(
	SELECT  date 
	       ,lineage_name 
	       ,lineage_wweid 
	       ,entity_type 
	       ,brand 
	       ,role 
	       ,duration
	FROM {{source 
	('fds_cpg', 'rpt_daily_talent_equity_centralized' 
	)}}
	GROUP BY  1 
	         ,2 
	         ,3 
	         ,4 
	         ,5 
	         ,6 
	         ,7 
) a
LEFT JOIN {{source 
('fds_mdm', 'characterlineage_grouplineage' 
)}} q
ON a.lineage_name=q.group_lineage_name
GROUP BY  1 
         ,2 
         ,3