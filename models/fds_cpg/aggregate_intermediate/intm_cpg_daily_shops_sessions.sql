{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'ephemeral','tags': "Phase 5B"
  })
}}

SELECT
    date,
    property,
    CASE
        WHEN channel_Grouping = 'Paid Search' AND trafficsource_source SIMILAR TO '%(Facebook|facebook|instagram)%'
        THEN 'Paid Social'
        WHEN trafficsource_source = 'Bluecore'
        THEN 'Email'
        WHEN trafficsource_source SIMILAR TO '%(wwe.com|help.wwe.com|WWE)%'
        THEN 'WWE.com'
        WHEN channel_Grouping = 'Referral'
        THEN 'Other'
        WHEN channel_Grouping = '(Other)'
        THEN 'Other'
        WHEN channel_Grouping = 'Other Advertising'
        THEN 'Other'
        ELSE channel_Grouping
    END AS Channel ,
    visit_id_calc,
    total_visits,
    trafficsource_source,
    total_transactions,
    total_totalTransactionRevenue
FROM
    {{source('hive_udl_cpg','ga_cpg_daily_sessions')}}
WHERE
    date >= '{{ var("start_date") }}' and date <= '{{ var("end_date") }}'
