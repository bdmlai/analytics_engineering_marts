{{
  config({
		'schema': 'fds_cp',
		"materialized": 'table','tags': "rpt_cp_daily_int_kpi_ranking_tv","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": "grant select on {{this}} to public"
  })
}}

SELECT
    week,
    DATE,
    country,
    region,
    program,
    series_type,
    telecasts,
    telecast_hours,
    weekly_aud,
    duration_mins,
    population,
    'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id,
    'bi_dbt_user_prd'                                   AS etl_insert_user_id,
    SYSDATE                                                etl_insert_rec_dttm,
    ''                                                     etl_update_user_id,
    SYSDATE                                                etl_update_rec_dttm
FROM
    (
        SELECT
            c.week,
            c.date,
            c.country,
            d.region,
            c.program,
            c.series_type,
            SUM(c.telecasts)      AS telecasts,
            SUM(c.telecast_hours) AS telecast_hours,
            SUM(c.weekly_aud)     AS weekly_aud,
            AVG(c.duration_mins)  AS duration_mins,
            AVG(d.population)     AS population
        FROM
            (
                SELECT
                    a.week,
                    a.month AS DATE,
                    a.country,
                    a.program,
                    a.series_type,
                    SUM(a.telecasts_count) AS telecasts,
                    SUM(b.telecast_hours)  AS telecast_hours,
                    SUM(a.weekly_aud)      AS weekly_aud,
                    AVG(b.duration_mins)   AS duration_mins,
                    AVG(b.aud)             AS aud
                FROM
                    {{ref('intm_cp_kpi_ranking_tv_wca_date')}} a
                LEFT JOIN
                    {{ref('intm_cp_kpi_ranking_tv_vh_date')}} b
                ON
                    LOWER(a.country)=LOWER(b.country)
                AND LOWER(a.program)=LOWER(b.program)
                AND a.week=b.week
                    -- Added series_type in joining condition
                    -- Jira PSTA-3617
                AND LOWER(a.series_type)=LOWER(b.series_Type)
                GROUP BY
                    1,2,3,4,5
                ORDER BY
                    1,2,3,4,5 )c
        LEFT JOIN
            {{ref('intm_cp_kpi_ranking_tv_region_mapping')}} d
        ON
            LOWER(c.country)=LOWER(d.country)
        GROUP BY
            1,2,3,4,5,6
        ORDER BY
            1,2,3,4,5,6 )