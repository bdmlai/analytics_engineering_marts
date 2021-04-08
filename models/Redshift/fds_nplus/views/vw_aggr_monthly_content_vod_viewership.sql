{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'view','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": ["grant select on {{this}} to public"]
  })
}}
select a.production_id,
a.dim_stream_type_id,
a.dim_content_id,
a.content_title,
a.class_name,
a.category_name,
a.event_style_name,
a.series_name,
a.series_group,
a.subs_tier,
a.content_tier,
a.mnth_start_dt,
a.mnth_end_dt,
a.unique_viewer_cnt,
a.view_cnt,
a.rank,
a.as_on_datekey,
a.as_on_date,
a.etl_batch_id,
a.etl_insert_user_id,
a.etl_insert_rec_dttm,
a.etl_update_user_id,
a.etl_update_rec_dttm,
a.dim_country_id,
a.country_cd,
b.region_nm,case when b.region_nm = 'United States' then 'domestic' else 'international' end as region_type
from {{source('fds_nplus','aggr_monthly_content_vod_viewership')}} a
left outer join {{source('cdm','dim_region_country')}} b
on a.dim_country_id = b.dim_country_id
where b.ent_map_nm = 'GM Region'