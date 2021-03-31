{{
  config({
		"materialized": 'ephemeral'
  })
}}

     
            SELECT 
            timestamp 'epoch' + ((datediff(minute,'1970-01-01 00:00:00',end_time::timestamp) / 5 * 5)+5) * interval '1 minute'  
            as end_timestamp,
            *
            FROM {{ref('intm_nplus_aa_impressions_raweventlog_encompass')}}

