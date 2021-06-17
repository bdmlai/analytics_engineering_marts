{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT event_date,
    event_dttm,
    event_name,
    event_type,
    event_reporting_type,
    adds_days_to_event,
    adds_day_of_week,
    adds_date,
    adds_time - extract(
        hour
        FROM event_dttm
    ) AS adds_time_to_event,
    adds_time,
    paid_adds,
    trial_adds,
    total_adds
FROM {{ref('intm_ppv_rpt_ppv_final_table')}}
WHERE adds_time is NOT null
UNION all
(
    SELECT a.event_date,
        a.event_dttm,
        a.event_name,
        a.event_type,
        a.event_reporting_type,
        a.adds_days_to_event,
        a.adds_day_of_week,
        a.adds_date,
        b.adds_time - extract(
            hour
            FROM a.event_dttm
        ) AS adds_time_to_event,
        b.adds_time,
        b.total_adds AS paid_adds,
        0 AS trial_adds,
        b.total_adds
    FROM {{ref('intm_ppv_rpt_ppv_final_table')}} AS a
        INNER JOIN (
            SELECT adds_date,
                adds_time,
                paid_adds,
                trial_adds,
                total_adds
            FROM (
                    SELECT date AS adds_date,
                        hour adds_time,
                        sum(paid_adds) paid_adds,
                        sum(trial_adds) trial_adds,
                        sum(paid_adds) + sum(trial_adds) total_adds
                    FROM {{source('fds_nplus', 'drvd_intra_hour_quarter_hour_adds')}}
                    WHERE date =(
                            case
                                WHEN extract(
                                    hour
                                    FROM convert_timezone(
                                            'AMERICA/NEW_YORK',
                                            cast(current_timestamp AS timestamp)
                                        )
                                ) = 0 THEN trunc(convert_timezone('AMERICA/NEW_YORK', sysdate -1))
                                ELSE trunc(convert_timezone('AMERICA/NEW_YORK', sysdate))
                            END
                        )
                    GROUP BY 1,
                        2
                )
            WHERE adds_time <(
                    case
                        WHEN extract(
                            hour
                            FROM convert_timezone(
                                    'AMERICA/NEW_YORK',
                                    cast(current_timestamp AS timestamp)
                                )
                        ) = 0 THEN 24
                        ELSE extract(
                            hour
                            FROM convert_timezone(
                                    'AMERICA/NEW_YORK',
                                    cast(current_timestamp AS timestamp)
                                )
                        )
                    end
                )
        ) AS b ON a.adds_date = b.adds_date
    WHERE a.adds_time is null
)