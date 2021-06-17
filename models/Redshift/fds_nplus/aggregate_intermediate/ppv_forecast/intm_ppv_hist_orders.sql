{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT adds_date,
    adds_time,
    trial_adds,
    paid_adds,
    trial_adds + Paid_adds total_adds
FROM (
        SELECT ordr_dt adds_date,
            hour_flag adds_time,
            COALESCE(Paid_add, 0) AS Paid_adds,
            COALESCE(Trial_add, 0) AS Trial_adds
        FROM (
                SELECT ordr_dt,
                    hour_flag,
                    sum(
                        case
                            WHEN ADD_TYPE = 'Trial' THEN add_cnt
                        end
                    ) AS Trial_add,
                    sum(
                        case
                            WHEN ADD_TYPE = 'Paid' THEN add_cnt
                        end
                    ) AS Paid_add
                FROM (
                        SELECT trunc(initial_order_dttm) ordr_dt,
                            date_part(hour, initial_order_dttm) hour_flag,
                            CASE
                                WHEN trial_start_dttm is NOT NULL THEN 'Trial'
                                ELSE 'Paid'
                            END AS add_type,
                            count(distinct order_id) add_cnt
                        FROM {{source('fds_nplus','fact_daily_subscription_order_status')}}
                        WHERE trunc(initial_order_dttm) IN (
                                SELECT DISTINCT adds_date
                                FROM {{ref('intm_ppv_full_list')}}
                            )
                            AND billing_country_cd NOT IN ('usd', 'us')
                        GROUP BY 1,
                            2,
                            3
                    )
                GROUP BY 1,
                    2
            )
    )