{{
  config({
        'schema': 'fds_nplus',
		"materialized": 'table',"tags": 'Phase0',"persist_docs": {'relation' : true, 'columns' : true},
            "post-hook" : 'grant select on {{this}} to public'
  })
}}


SELECT  trunc(initial_order_date) AS order_date,
        CASE 
            WHEN order_type='first' THEN 'NEW' 
            ELSE 'WINBACK' 
        END                       AS order_type,
        CASE 
            WHEN trial_new_add_date IS NOT NULL THEN 'Trial' 
            ELSE 'Paid' 
        END                       AS add_type,
        region_nm                 AS region,
        country_nm                AS country,
        currency_cd               AS currency,
        COUNT(order_id)           AS adds,
        COUNT(  
                CASE 
                    WHEN trial_to_paid_conversion_date >= initial_order_date THEN order_id 
                END
             )                    AS trial_converts
FROM    {{ref('intm_nplus_daily_sos_adds')}}
GROUP BY 1,2,3,4,5,6