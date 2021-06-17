{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT event_reporting_type,
    event_name,
    event_date,
    event_dttm,
    current_adds_date,
    current_adds_days_to_event,
    current_adds_day_of_week,
    current_adds_time,
    ghw_adds_tillnow,
    currentday_adds_tillnow,
    weekend_adds_tillnow,
    ghw_adds_tillnow1,
    (
        SELECT d2_pct
        FROM {{ref('intm_ppv_rpt_ppv_hourly_pct')}}
        WHERE adds_time = current_adds_time
            AND adds_day_of_week = current_adds_day_of_week
    ) d2_pct1,
    --current_adds_day_of_week Nik Except sunday 
    (
        SELECT d2_pct
        FROM {{ref('intm_ppv_rpt_ppv_hourly_pct')}}
        WHERE adds_time = 23
            AND adds_day_of_week = current_adds_day_of_week
    ) d2_pct2,
    --current_adds_day_of_week Nik Except sunday
    CASE
        WHEN ghw_adds_tillnow > 0
        AND comp_ghw_adds_tillnow > 0 THEN ghw_adds_tillnow /(comp_ghw_adds_tillnow / comp_ghw_adds)
        ELSE -1
    END AS ghw_adds_estimate,
    (
        SELECT distinct CASE
                WHEN event_type = 'current_ppv'
                AND adds_day_of_week = 'Saturday' -- adds_day_of_week THEN
                (
                    SELECT sum(total_adds)
                    FROM {{ref('intm_ppv_final_view')}}
                    WHERE event_type = 'current_ppv'
                        AND adds_days_to_event = -2
                )
                WHEN event_type = 'current_ppv'
                AND adds_day_of_week = 'Sunday' --adds_day_of_week Nik Except sunday THEN
                (
                    SELECT sum(total_adds)
                    FROM {{ref('intm_ppv_final_view')}}
                    WHERE event_type = 'current_ppv'
                        AND adds_days_to_event BETWEEN -2 AND -1
                )
                ELSE 0
            END AS ghw_adds_tillnow2
        FROM {{ref('intm_ppv_final_view')}}
        WHERE event_type = 'current_ppv'
            AND trunc(adds_date) =case
                WHEN extract(
                    hour
                    FROM convert_timezone(
                            'AMERICA/NEW_YORK',
                            cast(current_timestamp AS timestamp)
                        )
                ) = 0 THEN trunc(
                    convert_timezone(
                        'AMERICA/NEW_YORK',
                        cast(current_timestamp -1 AS timestamp)
                    )
                )
                ELSE trunc(convert_timezone('AMERICA/NEW_YORK', sysdate))
            end
    ) ghw_adds_tillnow2
FROM (
        SELECT max(event_reporting_type) AS event_reporting_type,
            max(
                case
                    WHEN event_type = 'current_ppv' THEN event_name
                    ELSE NULL
                end
            ) AS event_name,
            max(
                case
                    WHEN event_type = 'current_ppv' THEN event_date
                    ELSE NULL
                end
            ) AS event_date,
            max(
                case
                    WHEN event_type = 'current_ppv' THEN event_dttm
                    ELSE NULL
                end
            ) AS event_dttm,
            max(
                case
                    WHEN current_days_to_event = 1
                    AND event_type = 'current_ppv' THEN adds_date
                    ELSE NULL
                end
            ) AS current_adds_date,
            max(
                case
                    WHEN current_days_to_event = 1
                    AND event_type = 'current_ppv' THEN adds_days_to_event
                    ELSE NULL
                end
            ) AS current_adds_days_to_event,
            max(
                case
                    WHEN current_days_to_event = 1
                    AND event_type = 'current_ppv' THEN adds_day_of_week
                    ELSE NULL
                end
            ) AS current_adds_day_of_week,
            max(
                case
                    WHEN current_days_to_event = 1
                    AND current_time_to_event = 1
                    AND event_type = 'current_ppv' THEN adds_time
                    ELSE NULL
                end
            ) AS current_adds_time,
            sum(
                case
                    WHEN event_type = 'current_ppv'
                    AND adds_days_to_event BETWEEN -2 AND 0 THEN total_adds::float
                    ELSE 0
                end
            ) AS weekend_adds_tillnow,
            sum(
                case
                    WHEN event_type = 'current_ppv'
                    AND adds_days_to_event BETWEEN -6 AND 0 THEN total_adds::float
                    ELSE 0
                end
            ) AS ghw_adds_tillnow,
            sum(
                case
                    WHEN event_type = 'current_ppv'
                    AND adds_days_to_event BETWEEN -6 AND -1 THEN total_adds::float
                    ELSE 0
                end
            ) AS ghw_adds_tillnow1,
            sum(
                case
                    WHEN current_days_to_event = 1
                    AND event_type = 'current_ppv' THEN total_adds::float
                    ELSE 0
                end
            ) AS currentday_adds_tillnow,
            sum(
                case
                    WHEN current_time_to_event = 1
                    AND event_type != 'current_ppv'
                    AND adds_days_to_event BETWEEN -6 AND 0 THEN total_adds::float
                    ELSE 0
                end
            ) AS comp_ghw_adds_tillnow,
            sum(
                case
                    WHEN event_type != 'current_ppv'
                    AND adds_days_to_event BETWEEN -6 AND 0 THEN total_adds::float
                    ELSE 0
                end
            ) AS comp_ghw_adds
        FROM (
                SELECT a.*,
                    CASE
                        WHEN b.adds_days_to_event is NULL THEN 0
                        ELSE 1
                    END AS current_days_to_event,
                    CASE
                        WHEN c.adds_time_to_event is NULL THEN 0
                        ELSE 1
                    END AS current_time_to_event,
                    a.adds_time_to_event
                FROM {{ref('intm_ppv_final_view')}} AS a
                    LEFT JOIN (
                        SELECT max(adds_days_to_event) AS adds_days_to_event
                        FROM {{ref('intm_ppv_final_view')}}
                        WHERE event_type = 'current_ppv'
                            AND total_adds is NOT NULL
                    ) AS b ON a.adds_days_to_event = b.adds_days_to_event
                    LEFT JOIN (
                        SELECT DISTINCT adds_days_to_event,
                            adds_time_to_event
                        FROM {{ref('intm_ppv_final_view')}}
                        WHERE event_type = 'current_ppv'
                            AND total_adds is NOT NULL
                    ) AS c ON a.adds_days_to_event = c.adds_days_to_event
                    AND a.adds_time_to_event = c.adds_time_to_event
            )
    ) a