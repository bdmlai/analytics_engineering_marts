


{{
  config({
		"materialized": 'ephemeral'
  })
}}



                        SELECT
                            filter_name,
                            concurrent_plays,
                            ts2,
                            lead(concurrent_plays) OVER (partition BY filter_name ORDER BY ts2) AS
                                                                                      plays_upp,
                            lead(ts2) OVER (partition BY filter_name ORDER BY ts2) AS ts2_upp
                        FROM
                            (
                                SELECT filter_name,
                            concurrent_plays,
                            ts2
					 FROM
                                    (
                                        SELECT filter_name,
                            concurrent_plays,
					 pulse_timestamp AS ts2 ,
                                            rank() over( partition BY pulse_timestamp,filter_name
                                            ORDER BY as_on_date DESC ) AS rank
                                        FROM
                                            {{source('hive_udl_ads','conviva_daily_pulse')}}
                                        WHERE
                                            pulse_timestamp::DATE >= '2020-06-01' )
                                WHERE
                                    rank = 1 )