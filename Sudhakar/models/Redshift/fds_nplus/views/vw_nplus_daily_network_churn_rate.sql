
 {{
  config({
	"schemas": 'fds_nplus',	
	"materialized": 'view',"persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on {{this}} to public'

		})
}}



SELECT   bill_date AS loss_date,
         order_type,
         provider,
         country_ind,
         CASE
                  WHEN paid_mths_since_order<=8 THEN Cast(paid_mths_since_order AS VARCHAR)
                  WHEN paid_mths_since_order<13 THEN '9-12'
                  WHEN paid_mths_since_order<17 THEN '13-16'
                  WHEN paid_mths_since_order<21 THEN '17-20'
                  WHEN paid_mths_since_order<30 THEN '21-29'
                  ELSE '30+'
         END AS mso_group,
         Sum(Nvl(paid_end_actives,0)  +Nvl(other_terminations,0)+Nvl(non_renewal_loss,0)+Nvl(payment_failure,0)) AS up_for_renewal,
         Sum(Nvl(other_terminations,0)+Nvl(non_renewal_loss,0)+Nvl(payment_failure,0)) AS loss
FROM {{source('fds_nplus','aggr_forecast_dice_daily_summary')}} 
GROUP BY 1,2,3,4,5