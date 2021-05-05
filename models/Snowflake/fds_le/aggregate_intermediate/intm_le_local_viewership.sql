{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    interval_start_date_id,
    src_series_name,
    geography,
    AVG(rtg_percent)        AS rtg_percent,
    SUM(ue::DECIMAL(20,4))  AS ue,
    SUM(imp::DECIMAL(20,4)) AS imp
FROM
    {{source('prod_entdwdb.fds_nl','fact_nl_monthly_local_market')}}
WHERE
    src_playback_period_desc = 'Live+Same Day'
AND src_series_name IN ('WWE SMACKDOWN',
                        'WWE ENTERTAINMENT')
AND interval_start_date_id >= 20190101
AND src_demographic_group = 'TV Households P2+'
GROUP BY
    1,2,3