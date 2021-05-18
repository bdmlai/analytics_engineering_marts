{{
  config({
        'schema': 'fds_kntr',
		"materialized": 'table',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true},
            "post-hook" : 'grant select on {{this}} to public'
  })
}}

SELECT  src_country,
        broadcast_network_prem_type,
        rpt_yr,
        rpt_mn,
        'AWCA'      AS metric,
        sum(AWCA)   AS value
FROM {{ref('intm_kntr_monthly_india_tv_ppv_final')}}
GROUP BY 1,2,3,4
UNION ALL
SELECT  src_country,
        broadcast_network_prem_type,
        rpt_yr,
        rpt_mn,
        'telecast hours'    AS metric,
        sum(telecast_hours) AS value
FROM {{ref('intm_kntr_monthly_india_tv_ppv_final')}}
GROUP BY 1,2,3,4
UNION ALL
SELECT  src_country,
        broadcast_network_prem_type,
        rpt_yr,
        rpt_mn,
        'viewer hours'      AS metric,
        sum(viewer_hours)   AS value
FROM {{ref('intm_kntr_monthly_india_tv_ppv_final')}}
GROUP BY 1,2,3,4