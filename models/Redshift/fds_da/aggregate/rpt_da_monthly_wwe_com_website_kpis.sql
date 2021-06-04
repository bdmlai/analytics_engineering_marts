 {{ config({ 
      "schema": 'fds_da', 
      "materialized": 'table',"tags": 'rpt_da_monthly_wwe_com_website_kpis', 
	  "persist_docs": {'relation' : true, 'columns' : true}, 
      'post-hook': 'grant SELECT ON {{ this }} to public' }) }} 


select * from

( select * from {{ref("intm_da_kpi_website_union")}} 
where (metric_value_flag<>'fail') and 
(page_type<>'All' OR 
trafficsource_channel<>'All' OR 
landing_page_type<>'All') AND 
date >= '2019-01-01' 

union 

select * from {{ref("intm_da_kpi_website_union")}} 
where (metric_value_flag<>'fail') AND 
page_type='All' AND 
trafficsource_channel='All' AND 
landing_page_type='All')