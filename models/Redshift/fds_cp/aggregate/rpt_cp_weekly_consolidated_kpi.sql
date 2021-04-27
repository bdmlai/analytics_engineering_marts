{{
  config({
		 'schema': 'fds_cp',
	        "materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi',"persist_docs": {'relation' : true, 'columns' : true},
               'post-hook': ["grant select on {{this}} to public",
                           "drop table dt_prod_support.intm_dim_dates",
						   "drop table dt_prod_support.intm_dp_wkly_nw_glbl",
						   "drop table dt_prod_support.intm_dp_wkly_nw_intl",
						   "drop table dt_prod_support.intm_dp_wkly_fb",
						   "drop table dt_prod_support.intm_dp_wkly_tt",
						   "drop table dt_prod_support.intm_dp_wkly_dc",
						   "drop table dt_prod_support.intm_dp_wkly_tw",
						   "drop table dt_prod_support.intm_dp_wkly_ig",
						   "drop table dt_prod_support.intm_dp_wkly_sc",
						   "drop table dt_prod_support.intm_dp_wkly_yt",
						   "drop table dt_prod_support.intm_dp_wkly",
						   "drop table dt_prod_support.intm_dp_wkly1",
						   "drop table dt_prod_support.intm_dp_mthly",
						   "drop table dt_prod_support.intm_dp_mthly1",
						   "drop table dt_prod_support.intm_dp_yrly",
						   "drop table dt_prod_support.intm_dp_yrly1",
						   "drop table dt_prod_support.intm_dp_wkly_pivot",
						   "drop table dt_prod_support.intm_dp_mthly_pivot",
						   "drop table dt_prod_support.intm_dp_yrly_pivot",
						   "drop table dt_prod_support.intm_consolidation",
						   "drop table dt_prod_support.intm_final"
                            ]
                           	
        })
}}

SELECT a.granularity,
         a.platform,
         a.type,
         a.metric,
         a.year,
         a.month,
         a.week,
         a.start_date,
         a.end_date,
        
    CASE
    WHEN a.granularity = 'MTD'
        AND a.platform = 'Network'
        AND a.type='GLOBAL'
        AND a.metric = 'Active Viewers' THEN
    b.cntd_views_mtd
    WHEN a.granularity = 'MTD'
        AND a.platform = 'Network'
        AND a.type='INTL'
        AND a.metric = 'Active Viewers' THEN
    b.cntd_views_mtd_intl
    WHEN a.granularity = 'YTD'
        AND a.platform = 'Network'
        AND a.type='GLOBAL'
        AND a.metric = 'Active Viewers' THEN
    d.cntd_views_ytd
    WHEN a.granularity = 'YTD'
        AND a.platform = 'Network'
        AND a.type='INTL'
        AND a.metric = 'Active Viewers' THEN
    d.cntd_views_ytd_intl
    ELSE a.value
    END AS value, a.prev_year, a.prev_year_week, a.prev_year_start_date, a.prev_year_end_date,
    CASE
    WHEN a.granularity = 'MTD'
        AND a.platform = 'Network'
        AND a.type='GLOBAL'
        AND a.metric = 'Active Viewers' THEN
    c.prev_cntd_views_mtd
    WHEN a.granularity = 'MTD'
        AND a.platform = 'Network'
        AND a.type='INTL'
        AND a.metric = 'Active Viewers' THEN
    c.prev_cntd_views_mtd_intl
    WHEN a.granularity = 'YTD'
        AND a.platform = 'Network'
        AND a.type='GLOBAL'
        AND a.metric = 'Active Viewers' THEN
    e.prev_cntd_views_ytd
    WHEN a.granularity = 'YTD'
        AND a.platform = 'Network'
        AND a.type='INTL'
        AND a.metric = 'Active Viewers' THEN
    e.prev_cntd_views_ytd_intl
    ELSE a.prev_year_value
    END AS prev_year_value
FROM {{ref('intm_final')}} a
LEFT JOIN 
    (SELECT a.granularity,
         a.platform,
         a.type,
         a.metric,
         a.year,
         a.month,
         a.week,
         a.start_date,
         a.end_date,
         count(distinct (case
        WHEN b.country_cd <> 'united states' THEN
        b.src_fan_id
        ELSE NULL end)) AS cntd_views_mtd_intl, count(distinct b.src_fan_id) AS cntd_views_mtd
    FROM {{ref('intm_final')}} a
    LEFT JOIN {{source('fds_nplus','fact_daily_content_viewership')}} b
        ON trunc(b.stream_start_dttm)
        BETWEEN a.start_date
            AND a.end_date
    WHERE b.subs_tier = '2'
            AND trunc(b.stream_start_dttm) >= '2020-06-01'
            AND a.platform = 'Network'
            AND a.metric = 'Active Viewers'
            AND a.granularity = 'MTD'
    GROUP BY  1,2,3,4,5,6,7,8,9 ) b
    ON a.granularity = b.granularity
        AND a.platform = b.platform
        AND a.type = b.type
        AND a.metric = b.metric
        AND a.year = b.year
        AND a.month = b.month
        AND a.week = b.week
        AND a.start_date = b.start_date
        AND a.end_date = b.end_date
LEFT JOIN 
    (SELECT a.granularity,
         a.platform,
         a.type,
         a.metric,
         a.year,
         a.month,
         a.week,
         a.start_date,
         a.end_date,
         count(distinct (case
        WHEN b.country_cd <> 'united states' THEN
        b.src_fan_id
        ELSE NULL end)) AS prev_cntd_views_mtd_intl, count(distinct b.src_fan_id) AS prev_cntd_views_mtd
    FROM {{ref('intm_final')}} a
    LEFT JOIN {{source('fds_nplus','fact_daily_content_viewership')}} b
        ON trunc(b.stream_start_dttm)
        BETWEEN a.prev_year_start_date
            AND a.prev_year_end_date
    WHERE b.subs_tier = '2'
            AND trunc(b.stream_start_dttm) >= '2020-06-01'
            AND a.platform = 'Network'
            AND a.metric = 'Active Viewers'
            AND a.granularity = 'MTD'
    GROUP BY  1,2,3,4,5,6,7,8,9 ) c
    ON a.granularity = c.granularity
        AND a.platform = c.platform
        AND a.type = c.type
        AND a.metric = c.metric
        AND a.year = c.year
        AND a.month = c.month
        AND a.week = c.week
        AND a.start_date = c.start_date
        AND a.end_date = c.end_date
LEFT JOIN 
    (SELECT a.granularity,
         a.platform,
         a.type,
         a.metric,
         a.year,
         a.month,
         a.week,
         a.start_date,
         a.end_date,
         count(distinct (case
        WHEN b.country_cd <> 'united states' THEN
        b.src_fan_id
        ELSE NULL end)) AS cntd_views_ytd_intl, count(distinct b.src_fan_id) AS cntd_views_ytd
    FROM {{ref('intm_final')}} a
    LEFT JOIN {{source('fds_nplus','fact_daily_content_viewership')}} b
        ON trunc(b.stream_start_dttm)
        BETWEEN a.start_date
            AND a.end_date
    WHERE b.subs_tier = '2'
            AND trunc(b.stream_start_dttm) >= '2020-06-01'
            AND a.platform = 'Network'
            AND a.metric = 'Active Viewers'
            AND a.granularity = 'YTD'
    GROUP BY  1,2,3,4,5,6,7,8,9 ) d
    ON a.granularity = d.granularity
        AND a.platform = d.platform
        AND a.type = d.type
        AND a.metric = d.metric
        AND a.year = d.year
        AND a.month = d.month
        AND a.week = d.week
        AND a.start_date = d.start_date
        AND a.end_date = d.end_date
LEFT JOIN 
    (SELECT a.granularity,
         a.platform,
         a.type,
         a.metric,
         a.year,
         a.month,
         a.week,
         a.start_date,
         a.end_date,
         count(distinct (case
        WHEN b.country_cd <> 'united states' THEN
        b.src_fan_id
        ELSE NULL end)) AS prev_cntd_views_ytd_intl, count(distinct b.src_fan_id) AS prev_cntd_views_ytd
    FROM {{ref('intm_final')}} a
    LEFT JOIN {{source('fds_nplus','fact_daily_content_viewership')}} b
        ON trunc(b.stream_start_dttm)
        BETWEEN a.prev_year_start_date
            AND a.prev_year_end_date
    WHERE b.subs_tier = '2'
            AND trunc(b.stream_start_dttm) >= '2020-06-01'
            AND a.platform = 'Network'
            AND a.metric = 'Active Viewers'
            AND a.granularity = 'YTD'
    GROUP BY  1,2,3,4,5,6,7,8,9 ) e
    ON a.granularity = e.granularity
        AND a.platform = e.platform
        AND a.type = e.type
        AND a.metric = e.metric
        AND a.year = e.year
        AND a.month = e.month
        AND a.week = e.week
        AND a.start_date = e.start_date
        AND a.end_date = e.end_date
ORDER BY  a.platform, a.granularity, a.metric, a.year, a.week