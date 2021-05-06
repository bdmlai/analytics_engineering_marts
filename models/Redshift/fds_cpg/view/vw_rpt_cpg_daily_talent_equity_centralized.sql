{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'monthly_talent_scorecard',
		 "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": 'grant select on {{this}} to public'
        })
}}

SELECT  date 
       ,lineage_name 
       ,brand 
       ,SUM(demand_sales) AS demand_sales
FROM 
(
	SELECT  date 
	       ,lineage_name 
	       ,brand 
	       ,src_style_description 
	       ,demand_units 
	       ,demand_sales
	FROM {{source 
	('fds_cpg', 'rpt_daily_talent_equity_centralized' 
	)}}
	GROUP BY  1 
	         ,2 
	         ,3 
	         ,4 
	         ,5 
	         ,6 
)
GROUP BY  1 
         ,2 
         ,3