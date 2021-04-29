{{
  config({
		 'schema': 'fds_nplus',	
	     "materialized": 'view',
		"post-hook" : 'grant select on {{this}} to public'

        })
}}
--vw_rpt_nplus_weekly_ppv_language_vod_viewership
SELECT
    a.production_id,
    a.episode_nm,
    a.debut_date,
    a.as_on_date,
    a.tier,
    INITCAP(NVL(a.language,'Other')) AS language,
    NVL(b.country_nm, 'Other')       AS country,
    NVL(b.region_nm, 'Other')        AS region,
    a.time_slot,
	a.view_day,
	a.unique_viewer_cnt
FROM
    {{ref('rpt_nplus_weekly_ppv_language_vod_viewership')}} a
LEFT JOIN
    {{source('cdm','dim_region_country')}} b
ON
    a.dim_country_id=b.dim_country_id
AND ent_map_nm='GM Region'
AND src_sys_cd='iso'
where a.as_on_date = (select max(as_on_date) from {{ref('rpt_nplus_weekly_ppv_language_vod_viewership')}})