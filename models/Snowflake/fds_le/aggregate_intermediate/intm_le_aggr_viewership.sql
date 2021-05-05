{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    AVG(ue) over(partition BY src_series_name ORDER BY interval_start_date_id ASC rows 2 preceding)
    AS ue_3m_avg_nat,
    AVG(imp) over(partition BY src_series_name ORDER BY interval_start_date_id ASC rows 2 preceding
    ) AS imp_3m_avg_nat
FROM
    (
        SELECT
            interval_start_date_id,
            src_series_name,
            (100.0000*SUM(imp)/NULLIF(SUM(ue), 0)) AS avg_audience_pct,
            SUM(ue)                                AS ue,
            SUM(imp)                               AS imp
        FROM
            {{ref('intm_le_local_viewership')}}
        GROUP BY
            1,2)