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
a.subs_year,
a.mnth_start_dt,
a.tot_active_subs,
a.lst_mnth_subs_viewing_cohort_rate,
a.lst2_mnth_subs_viewing_cohort_rate,
a.lst3_mnth_subs_viewing_cohort_rate,
a.lst_mnth_avg_hrs_subs_cohort,
a.lst2_mnth_avg_hrs_subs_cohort,
a.lst3_mnth_avg_hrs_subs_cohort,
a.lst2_mnth_active_subs,
a.lst3_mnth_active_subs,
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
a.lst_mnth_active_subs,
a.lst_mnth_subs_vieweing_cohort_rate_num,
a.lst2_mnth_subs_vieweing_cohort_rate_num,
a.lst3_mnth_subs_vieweing_cohort_rate_num,
a.lst_mnth_mnthly_total_hours_watched,
a.lst2_mnth_mnthly_total_hours_watched,
a.lst3_mnth_mnthly_total_hours_watched,
b.region_nm,case when b.region_nm = 'United States' then 'domestic' else 'international' end as region_type
from {{source('fds_nplus','aggr_monthly_subs_cohort_viewership')}} a
left outer join {{source('cdm','dim_region_country')}} b
on a.dim_country_id = b.dim_country_id
where b.ent_map_nm = 'GM Region'
