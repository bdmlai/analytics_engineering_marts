{{
  config({

		"materialized": 'ephemeral'
  })
}}

SELECT DISTINCT Cast(initial_order_date AS DATE) AS initial_order_date,
                country_cd,
                order_type,
                paid_new_add_date,
                loss_reason,
                paid_loss_date,
                trial_loss_date,
                payment_method,
                src_fan_id,
                order_id,
                as_on_date,
                payment_status
FROM  {{source('fds_nplus','fact_daily_subscription_status_plus')}}