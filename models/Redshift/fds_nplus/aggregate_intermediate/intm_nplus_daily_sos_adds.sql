{{
  config({
          
	"materialized": 'ephemeral'
  })
}}

SELECT  a.as_on_date,
        a.order_id, 
        a.dim_country_id,
        a.initial_order_date,
        a.paid_new_add_date,
        a.trial_new_add_date,
        a.trial_to_paid_conversion_date,
        a.order_type,
        a.currency_cd, 
        b.country_nm,
        b.region_nm 
FROM 
(
  (
        SELECT  as_on_date,
                order_id, 
                dim_country_id,
                initial_order_date,
                paid_new_add_date,
                trial_new_add_date,
                trial_to_paid_conversion_date,
                order_type,
                currency_cd
        FROM    {{source('fds_nplus','fact_daily_subscription_status_plus')}}
        WHERE   trunc(initial_order_date) >='2018-01-01'        AND
                trunc(as_on_date) = trunc(initial_order_date)+1
  ) a 
  INNER JOIN   {{source('cdm','dim_region_country')}} b
  ON    a.dim_country_id=B.DIM_COUNTRY_ID  AND 
        b.ent_map_nm = 'GM Region'
)