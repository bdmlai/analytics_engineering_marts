{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'Phase 5B',"persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on fds_cpg.vw_rpt_cpg_weekly_talent_top25_venue_sales to public'

        })
}}
select * from {{ref('rpt_cpg_weekly_talent_top25_venue_sales')}}