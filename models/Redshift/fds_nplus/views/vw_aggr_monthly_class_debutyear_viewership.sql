{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'view','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": ["grant select on {{this}} to public"]
  })
}}
select
a.month_start_date,
a.subs_tier,
a.debut_year_dt,
a.class_nm,
a.sameday_unique_viewer_cnt,
a.day7_unique_viewer_cnt,
a.day30_unique_viewer_cnt,
a.todate_unique_viewer_cnt,
a.sameday_hours_watched,
a.day7_hours_watched,
a.day30_hours_watched,
a.todate_hours_watched,
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
from {{source('fds_nplus','aggr_monthly_class_debutyear_viewership')}} a
left outer join {{source('cdm','dim_region_country')}} b
on a.dim_country_id = b.dim_country_id
where b.ent_map_nm = 'GM Region'