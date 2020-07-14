

  create view "entdwdb"."fds_nplus"."vw_aggr_daily_network_adds_and_loss_track__dbt_tmp" as (
    


with ttl_loss_data as
(select as_on_date-1 as as_on_date,country,order_status,cancel_type,payment_method,payments,ttl_losses
from (select as_on_date,
case when country_cd is null then '4.NA' when country_cd in ('united states', 'us' , 'usa' ) then '1.US'
when country_cd in ('united kingdom', 'ireland', 'isle of man', 'jersey', 'guernsey', 'je' , 'gg' , 'im' , 'ie') then '2.UK/IRE' else '3.ROW' end as country,
case when first_charged_date is null then 'Trial' else 'Paid' end as order_status,
case when revoked_date is null then 'Voluntary' else 'Involuntary' end as cancel_type,
case when payment_method like '%roku%' then 'Roku' when payment_method like '%paypal%' then 'Paypal'
    when product_sku like '%retail%' then 'Retail Card' else 'Managed Payment' end as payment_method,
case when payment_number between 0 and 1 then '0-1'
when payment_number between 2 and 3 then '2-3'
when payment_number between 4 and 6 then '4-6'
when payment_number between 7 and 12 then '7-12'
else '13+' end as payments,
COUNT(distinct(case when exprd_entlmnt_date = as_on_date-1 and payment_status = 'paid' then order_id end)) +
COUNT(distinct(case when exprd_entlmnt_date = as_on_date-1 and payment_status = 'trial' then order_id end)) as ttl_losses
from "entdwdb"."fds_nplus"."fact_daily_subscription_status_plus"
group by 1,2,3,4,5,6))
select * from ttl_loss_data
  ) ;
