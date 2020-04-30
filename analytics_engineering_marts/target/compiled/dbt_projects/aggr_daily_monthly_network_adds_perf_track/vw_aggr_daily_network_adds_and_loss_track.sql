

select trunc(entitlement_expiry_dttm) as date,
case when billing_sku_country_cd is null then '4.NA' when billing_sku_country_cd in ('united states', 'us' , 'usa' ) then '1.US' 
when billing_sku_country_cd in ('united kingdom', 'ireland', 'isle of man', 'jersey', 'guernsey', 'je' , 'gg' , 'im' , 'ie') 
then '2.UK/IRE' else '3.ROW' end as country,
case when first_charged_dttm is null then 'Trial' else 'Paid' end as order_status,
--using revoked_dttm since it's matching with legacy table
case when revoked_dttm is null then 'Voluntary' else 'Involuntary' end as cancel_type,
case when payment_method like '%roku%' then 'Roku' when payment_method like '%paypal%' then 'Paypal'  when product_sku like '%retail%' then 'Retail Card' else 'Managed Payment' end as payment_method,
case when payments between 0 and 1 then '0-1'
when payments between 2 and 3 then '2-3'
when payments between 4 and 6 then '4-6'
when payments between 7 and 12 then '7-12'
else '13+' end as payments_count,
count(distinct order_id) as total_losses
from (select * from(select *, max(payment_count) over (partition by order_id) as payments,row_number() over (partition by order_id order by as_on_date desc) as rk from fds_nplus.fact_daily_subscription_order_status) where rk=1)
where trunc(entitlement_expiry_dttm) >= '2019-09-01' and entitlement_expiry_dttm < as_on_date
group by 1,2,3,4,5,6 order by 1,2,3,4,5,6