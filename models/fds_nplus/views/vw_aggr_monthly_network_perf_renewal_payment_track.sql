{{
  config(
    schemas='fds_nplus',	
	materialized='view'
    
  )
}}

with ap as ( select * from (select order_id, extract (month from org_billing_dttm) as dt_mon,
trunc(org_billing_dttm) as ori_bill_dt , trunc(scheduled_billing_dttm) as sch_bill_dt ,
payment_status,active_status ,
case when billing_country_cd is null then '4.NA' when billing_country_cd in ('united states', 'us' , 'usa' ) then '1.US'
when billing_country_cd in ('united kingdom', 'ireland', 'isle of man', 'jersey', 'guernsey', 'je' , 'gg' , 'im' , 'ie')
then '2.UK/IRE' else '3.ROW' end as country,
case when payment_method = 'roku_iap'  then 'roku'
when product_sku like '%retail%' then 'RC' else 'managed payment' end
as payment_type,
case when revoked_dttm is null then 'Voluntary' else 'Involuntary' end as cancel_type,
case when payment_count between 0 and 1 then '0-1'
                when payment_count between 2 and 3 then '2-3'
                when payment_count between 4 and 6 then '4-6'
                when payment_count between 7 and 12 then '7-12'
                else '13+' end as Payments,row_number() over (partition by order_id, trunc(org_billing_dttm)  order by as_on_date desc) as rk,
max(case when revoked_dttm is null then 0 else 1 end ) as f_dt_ind ,
max(case when coalesce(trunc(revoked_dttm)) is not null  then 1 else 0 end) as invol_loss_ind,
max(case when trunc(scheduled_billing_dttm) > trunc(org_billing_dttm) then 1 else 0 end) as sf_ind
from {{source('fds_nplus','fact_daily_subscription_order_status')}} where trunc(initial_order_dttm) < trunc(org_billing_dttm)
group by 1,2,3,4,5,6,7,8,9,10,as_on_date)
where rk = 1)
select
dt_mon billing_month,
ori_bill_dt original_billing_date,
payment_status as order_status,
country,
payment_type payment_type,
count(order_id) as renewed_count ,
count(case when sch_bill_dt = ori_bill_dt and f_dt_ind = 1 and active_status <> 'active' then order_id end) as hard_fail_indicator,
sum(case when active_status = 'active' then 1 else 0  end) as renewed_total,
sum(case when active_status <> 'active' and f_dt_ind = 1 then 1 else 0  end) as involuntary_cancels_count,
sum(case when active_status <> 'active' and f_dt_ind = 0 then 1 else 0 end) as voluntary_cancels_count,
sum(sf_ind) as soft_fail_enter ,
sum(case when active_status = 'active' and sf_ind = 1 then 1 else 0  end) as soft_fail_successful_enter,
Payments payments_count,
cancel_type
from ap group by 1,2,3,4,5,Payments,cancel_type