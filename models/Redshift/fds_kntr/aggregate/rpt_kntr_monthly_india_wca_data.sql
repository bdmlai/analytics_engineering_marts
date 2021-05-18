{{
  config({
        'schema': 'fds_kntr',
		"materialized": 'table',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true},
            "post-hook" : 'grant select on {{this}} to public'
  })
}}

SELECT  src_country,
        series_name,
        broadcast_network_prem_type,
        rpt_yr,
        rpt_mn,
        live_flag,
        'AWCA'      AS metric,
        sum(AWCA)   AS value
FROM    {{ref('intm_kntr_monthly_india_tv_final')}}
WHERE   live_flag = 'N'
GROUP BY 1,2,3,4,5,6
UNION ALL
SELECT  src_country,
        series_name,
        broadcast_network_prem_type,
        rpt_yr,
        rpt_mn,
        live_flag,
        'AWCA Live' AS metric,
        sum(AWCA)   AS value
FROM    {{ref('intm_kntr_monthly_india_tv_final')}}
WHERE   live_flag = 'Y'
GROUP BY 1,2,3,4,5,6
UNION ALL
SELECT  src_country,
        series_name,
        broadcast_network_prem_type,
        rpt_yr,
        rpt_mn,
        live_flag,
        'telecast hours'    AS metric,
        sum(telecast_hours) AS value
FROM    {{ref('intm_kntr_monthly_india_tv_final')}}
GROUP BY 1,2,3,4,5,6
UNION ALL
SELECT  src_country,
        series_name,
        broadcast_network_prem_type,
        rpt_yr,
        rpt_mn,
        live_flag,
        'viewer hours'      AS metric,
        sum(viewer_hours)   AS value
FROM    {{ref('intm_kntr_monthly_india_tv_final')}}
GROUP BY 1,2,3,4,5,6