{{
  config({
		"materialized": 'ephemeral'
  })
}}

     

            SELECT t1.*, t2.filter_name as audience_type, t2.concurrent_plays
            FROM {{ref('intm_nplus_aa_impressions_raweventlog_encompass_join_time')}} t1
            LEFT JOIN (SELECT * , pulse_timestamp::timestamp - (5 * interval '1 minute') as ts1, 
            pulse_timestamp::timestamp as ts2 FROM {{source('hive_udl_ads','conviva_daily_pulse')}}
            WHERE pulse_timestamp::date >= '2017-01-01') t2 
            ON t1.end_timestamp = t2.ts2