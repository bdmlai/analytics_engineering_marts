{{
  config({
		"schema": 'fds_le',
		"materialized": 'table','tags': 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": 'grant select on {{this}} to public'
  })
}}
SELECT
    interval_start_date::VARCHAR(10) AS interval_start_date,
    state::VARCHAR(5)                AS state,
    geography,
    dma_name::VARCHAR(255)    AS dma_name,
    county_name::VARCHAR(255) AS county_name,
    src_series_name,
    brand_name,
    zip_code_count::INT                                      AS zip_code_count,
    rtg_percent::DECIMAL(32,4)                               AS rtg_percent,
    ue::INT                                                  AS ue,
    imp::INT                                                 AS imp,
    avg_audience_pct::DECIMAL(32,4)                          AS avg_audience_pct,
    (rtg_percent/NULLIF(avg_audience_pct, 0))::DECIMAL(32,4) AS INDEX,
    imp_3m_avg::DECIMAL(32,4)                                AS viewers_3m_avg,
    CASE
        WHEN src_series_name = 'WWE ENTERTAINMENT'
        THEN ((imp_3m_avg/NULLIF(ue_3m_avg, 0))/(imp_3m_avg_nat/NULLIF(ue_3m_avg_nat, 0)))::DECIMAL
            (32,4)
    END AS raw_index_3m_moving_avg,
    CASE
        WHEN src_series_name = 'WWE SMACKDOWN'
        THEN (rtg_percent/NULLIF(avg_audience_pct, 0))::DECIMAL(32,4)
    END AS SD_index,
    CASE
        WHEN src_series_name = 'WWE ENTERTAINMENT'
        THEN (rtg_percent/NULLIF(avg_audience_pct, 0))::DECIMAL(32,4)
    END AS RAW_index,
    CASE
        WHEN interval_start_date <= '2020-02-01'
        THEN 'Wave B'
        ELSE 'Wave A'
    END AS wave_flag,
    ('DBT_' || TO_CHAR(((convert_timezone ('UTC', GETDATE()))::TIMESTAMP),'YYYY_MM_DD_HH24_MI_SS')
    || '_5A')::VARCHAR(255)                            AS etl_batch_id,
    'SVC_PROD_BI_DBT_USER'                             AS etl_insert_user_id,
    ((convert_timezone ('UTC', GETDATE()))::TIMESTAMP) AS etl_insert_rec_dttm,
    NULL::VARCHAR(255)                                 AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
FROM
    (
        SELECT
            a.*,
            b.avg_audience_pct,
            b.ue_3m_avg_nat,
            b.imp_3m_avg_nat
        FROM
            {{ref('intm_le_local_county_mapped')}} a
        LEFT JOIN
            {{ref('intm_le_aggr_viewership')}} b
        ON
            a.interval_start_date_id = b.interval_start_date_id
        AND a.src_series_name = b.src_series_name)