
{{
  config({
		 'schema': 'fds_da',"materialized": 'view',"tags": 'rpt_da_monthly_wwe_com_website_kpis',"persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on fds_da.vw_rpt_da_monthly_wwe_com_website_kpis to public'

        })
}}
select date,
device_category,
device_type,
geonetwork_us_v_international,
page_type,
landing_page_type,
hit_social_share_type,
trafficsource_channel,
hit_video_attempt,
metric_name,
metric_value,
etl_row_chksum_txt,
etl_insert_rec_dttm,
etl_insert_user_id,
etl_source_name,
etl_job_id,
hit_next,
hit_isexit,
hit_scroll,
scroll_number,
pageview_count,
table_name,
metric_value_flag
 
 from {{ref('rpt_da_monthly_wwe_com_website_kpis')}}
