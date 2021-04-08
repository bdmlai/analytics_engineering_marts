{{
  config({
		 'schema': 'fds_nplus',	
	     "materialized": 'view',
		"post-hook" : 'grant select on {{this}} to public'

        })
}}

select a.dim_stream_type_id,
a.stream_type_cd,
a.dim_program_type_id,
a.program_type_cd,
a.initial_signup_year,
a.mnth_start_dt,
a.mnthly_unique_viewers_cnt,
a.mnthly_total_hours_watched,
a.mnthly_avg_hours_per_sub,
a.mnthly_avg_hours_per_viewer,
a.mnthly_subscriber_viewing_rate,
a.mnthly_avg_subscriber_cnt,
a.mnthly_max_subscriber_cnt,
a.as_on_datekey,
a.as_on_date,
a.subs_tier,
a.etl_batch_id,
a.etl_insert_user_id,
a.etl_insert_rec_dttm,
a.etl_update_user_id,
a.etl_update_rec_dttm,
a.dim_country_id,
a.country_cd,
b.region_nm,case when b.region_nm = 'United States' then 'domestic' else 'international' end as region_type
from {{source('fds_nplus','aggr_monthly_program_type_viewership')}} a
left outer join {{source('cdm','dim_region_country')}} b
on a.dim_country_id = b.dim_country_id
where b.ent_map_nm = 'GM Region'