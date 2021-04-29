{{
  config({
		"schema": 'fds_nl',
		"materialized": 'table',"tags": 'rpt_tv_weekly_consolidated_kpi',"persist_docs": {'relation' : true, 'columns' : true},
		'post-hook': ['grant select on {{ this }} to public',
						"drop table dt_prod_support.intm_nl_tv_wkly_wd2"	]
  })
}}


with final_tv as
(
select 		a.granularity, 
			a.platform, 
			a.type, 
			a.metric, 
			a.year, 
			a.month, 
			a.week, 
			a.start_date, 
			a.end_date, 
			a.value,
			a.prev_year, 
			a.prev_year_week, 
			a.prev_year_start_date, 
			a.prev_year_end_date, 
			a.prev_year_value

from 
(
select * from {{ ref("intm_nl_tv_wkly_pivot") }} 

union all 

select * from {{ ref("intm_nl_tv_mthly_pivot") }} 

union all 

select * from {{ ref("intm_nl_tv_yrly_pivot") }}

) a
)


select 		*,
			'dbt_'+to_char(convert_timezone('america/new_york', sysdate),'yyyy_mm_dd_hh_mi_ss')+'_cp' as etl_batch_id, 
			'bi_dbt_user_prd' as etl_insert_user_id,
			convert_timezone('america/new_york', sysdate) as etl_insert_rec_dttm,
			cast(null as varchar) as etl_update_user_id,
			cast( null as timestamp) as etl_update_rec_dttm 

from
(
select 		granularity, 
			platform, 
			type, 
			metric, 
			a.year, 
			a.month, 
			week, 
			case when granularity = 'MTD' then b.start_date 
				 when granularity = 'YTD' then c.start_date 
				 else a.start_date 
			end as start_date,
			end_date, 
			value, 
			prev_year, 
			prev_year_week, 
			case when granularity = 'MTD' then b.prev_year_start_date 
				 when granularity = 'YTD' then c.prev_year_start_date 
				 else a.prev_year_start_date 
			end as prev_year_start_date,     
			prev_year_end_date, 
			prev_year_value

from 		final_tv a

left join	(select year,month, min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from final_tv group by 1,2) b on a.year = b.year and a.month = b.month

left join	(select year,min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from final_tv group by 1) c on a.year = c.year

order by 	platform, granularity, metric, year, week
)