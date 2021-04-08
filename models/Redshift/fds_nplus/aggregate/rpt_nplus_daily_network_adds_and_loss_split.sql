{{
  config({
		"schema": 'fds_nplus',
                
		"materialized": 'incremental','tags': "rpt_nplus_daily_network_adds_and_loss_split" ,"persist_docs": {'relation' : true, 'columns' : true},
	        "post-hook" : 'grant select on {{this}} to public' 

  })
}}

SELECT       * ,
       'DBT_'+To_char(sysdate,'YYYY_MM_DD_HH_MI_SS')+'_NPLUS' AS etl_batch_id,
       'bi_dbt_user_prd' AS etl_insert_user_id,
       sysdate etl_insert_rec_dttm,
       '' etl_update_user_id,
       sysdate etl_update_rec_dttm
FROM   (
       (
                SELECT   Upper(payment_status) AS payment_status,
                         Upper(country_cd)     AS country_cd,
                         Upper(payment_method) AS payment_method,
                         Upper(order_type)     AS order_type,
                         CASE
                                  WHEN payment_status = 'paid' THEN paid_loss_date
                                  ELSE trial_loss_date
                         END AS date,
                         NULL  AS add_flag,
                         Upper(loss_reason) AS loss_reason,
                         CASE
                                  WHEN loss_reason IS NOT NULL THEN 'LOSSES'
                                  ELSE NULL
                         END  AS metric_type,
                         Count(DISTINCT src_fan_id) AS fan_count
                FROM (	
								SELECT  src_fan_id,								  
										country_cd,
										initial_order_date,
										registered_date,
										paid_new_add_date,
										payment_status,
										loss_reason,
										paid_loss_date,
										trial_loss_date,
										order_type,
										Upper(payment_method) AS payment_method,
										CASE
												WHEN d.initial_order_date = d.as_on_date - 1 AND d.order_type = 'winback' AND d.payment_status IN ('paid','trial') THEN 'winback'
												WHEN d.initial_order_date = d.as_on_date - 1 AND d.order_type = 'first' AND d.payment_status IN ('paid','trial') AND initial_order_date > d.registered_date AND    d.registered_date IS NOT NULL THEN 'prospect'
												WHEN d.initial_order_date = d.as_on_date - 1 AND d.order_type = 'first' AND d.payment_status IN ('paid','trial') AND initial_order_date = d.registered_date AND    d.registered_date IS NOT NULL THEN 'new_to_t3'
										END AS add_flag
								FROM   (									   
											SELECT  	a.*,
														b.registered_date
											FROM    (select * from  {{ref('intm_nplus_adds_and_loss_split')}}
													{% if is_incremental() %}
													WHERE  as_on_date BETWEEN To_date(Add_months(CURRENT_DATE, -6),'yyyy-mm-01') AND CURRENT_DATE
													{% endif %}
													)  a
											LEFT JOIN (SELECT * FROM   {{source('fds_nplus','fact_customer_login_and_status')}}
													WHERE  as_on_date = CURRENT_DATE - 1
													) b
											ON a.src_fan_id = b.src_fan_id
										)D 
					)
			 GROUP BY 1,2,3,4,5,6,7,8
	)
					   
	UNION ALL
	(                   
			SELECT   UPPER(payment_status) AS payment_status,
					UPPER(country_cd)     AS country_cd, 
					UPPER(payment_method) AS payment_method,   
					UPPER(order_type)     AS order_type,               
					initial_order_date    AS DATE,                     
					UPPER(add_flag)       AS add_flag,                  
					NULL                  AS loss_reason,                
					CASE  
						WHEN add_flag IS NOT NULL THEN 'ADDS'                                      
						ELSE NULL                            			
					END   AS metric_type,  
					COUNT(DISTINCT src_fan_id) AS fan_count         
			FROM   (
						SELECT	src_fan_id,
								country_cd,
								initial_order_date,
								registered_date,
								paid_new_add_date,
								payment_status,
								loss_reason,
								paid_loss_date,
								trial_loss_date,
								order_type,
								Upper(payment_method) AS payment_method,
								CASE
										WHEN d.initial_order_date = d.as_on_date - 1 AND d.order_type = 'winback' AND d.payment_status IN ('paid','trial') THEN 'winback'
										WHEN d.initial_order_date = d.as_on_date - 1 AND d.order_type = 'first' AND d.payment_status IN ('paid','trial') AND initial_order_date > d.registered_date AND    d.registered_date IS NOT NULL THEN 'prospect'
										WHEN d.initial_order_date = d.as_on_date - 1 AND d.order_type = 'first' AND d.payment_status IN ('paid','trial') AND initial_order_date = d.registered_date AND    d.registered_date IS NOT NULL THEN 'new_to_t3'
										END AS add_flag
						FROM   (									   
									SELECT  a.*,
											b.registered_date
									FROM    (select * from  {{ref('intm_nplus_adds_and_loss_split')}}
											{% if is_incremental() %}
											WHERE  as_on_date BETWEEN To_date(Add_months(CURRENT_DATE, -6),'yyyy-mm-01') AND CURRENT_DATE
											{% endif %}
											)  a
									LEFT JOIN	( SELECT * FROM   {{source('fds_nplus','fact_customer_login_and_status')}}
											      WHERE  as_on_date = CURRENT_DATE - 1
												) b
									ON a.src_fan_id = b.src_fan_id
								)D 
				)

			GROUP BY 1,2,3,4,5,6,7,8
	)
	)

	{% if is_incremental() %}
	WHERE     DATE >= to_Date(Add_months(CURRENT_DATE,-6),'yyyy-mm-01') - 1
	{% endif %}	 
				 				 