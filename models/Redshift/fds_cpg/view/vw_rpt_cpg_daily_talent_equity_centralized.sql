{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'monthly_talent_scorecard',
		 "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": 'grant select on {{this}} to public'
        })
}}

select date,lineage_name,brand,sum(demand_sales) as demand_sales from(
select date,lineage_name,brand,src_style_description,demand_units,demand_sales
from {{source('fds_cpg', 'rpt_daily_talent_equity_centralized')}} 
group by 1,2,3,4,5,6) group by 1,2,3