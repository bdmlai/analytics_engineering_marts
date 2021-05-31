{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'ppv_live_kickoff' }) }}


SELECT
		customerid      ,
		payload_data_cid,
		min_time        ,
		CASE WHEN max_time IS NULL THEN to_timestamp((CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern') AT TIME ZONE 'UTC', 'yyyy-mm-dd hh24:mi:ss') ELSE max_time END AS max_time
FROM
		(
				SELECT
						customerid               ,
						payload_data_cid         ,
						MIN(min_time) AS min_time,
						MAX(max_time) AS max_time
				FROM
						(
								SELECT *
										,
										(readable_startedat AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern' AS min_time,
										(readable_endedat AT   TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern' AS max_time
								FROM
										(
												SELECT DISTINCT
														customerid                                                                                                 ,
														payload_data_ta                                                                                            ,
														payload_data_cid                                                                                           ,
														TIMESTAMP 'epoch' + CAST(payload_data_startedat AS BIGINT)/1000 * interval '1 second' AS readable_startedat,
														TIMESTAMP 'epoch' + CAST(payload_data_endedat AS BIGINT)/1000 * interval '1 second'   AS readable_endedat  ,
														payload_data_device                                                                                        ,
														payload_data_last_active_at
												FROM
														{{source('udl_nplus','stg_dice_stream_flattened')}} 
												WHERE
														payload_data_ta IN ('LIVE_WATCHING_START',
																			'LIVE_WATCHING_END')
												AND     (((
																				TIMESTAMP 'epoch' + CAST(payload_data_startedat AS BIGINT)/1000 * interval '1 second') AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern') BETWEEN
														(
																SELECT
																		CAST(TRUNC(start_timestamp)AS TIMESTAMP)
																FROM
																		{{ ref("intm_nplus_live_manual_ppv") }}
																WHERE
																		platform    = 'Network'
																AND     event_brand = 'PPV')
												AND
														(
																SELECT
																		DATEADD(m,2,end_timestamp)
																FROM
																		{{ ref("intm_nplus_live_manual_ppv") }}
																WHERE
																		platform    = 'Network'
																AND     event_brand = 'PPV')) )
				GROUP BY
						customerid,
						payload_data_cid ) 