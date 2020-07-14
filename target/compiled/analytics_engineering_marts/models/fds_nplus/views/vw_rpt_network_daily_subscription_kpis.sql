
SELECT c.country_nm,a.*, 
b.domest_paid_active,
b.domest_trial_active,
b.inter_paid_active,
b.inter_trial_active,
b.active,
b.inactive,
b.lp_active,
b.t2_inact_dedup,
b.t2_inact_dedup_logged_in,
b.prospects_logged_in,
b.prospects_not_logged_in,
b.unique_visitors_mtd_prospects_inactives,
b.unique_visitors_network_subscribers_mtd
 from
(select as_on_date-1 as date,dim_country_id,country_cd,
sum(case when order_type='first' then daily_paid_adds_cnt else null end) as daily_paid_adds_cnt_new,
sum(case when order_type='winback' then daily_paid_adds_cnt else null end) as daily_paid_adds_cnt_winback,
sum(case when order_type='first' and payment_method not in ('china_pptv', 'osn', 'rogers', 'astro')then daily_paid_loss_cnt else null end) as daily_paid_loss_cnt_new,
sum(case when order_type='winback' and payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') then daily_paid_loss_cnt else null end) as daily_paid_loss_cnt_winback,

sum(case when country_cd='us' then daily_paid_adds_cnt else null end) as domest_paid_add,
sum(case when country_cd='us' then daily_trial_adds_cnt else null end) as domest_trial_add,
sum(case when country_cd !='us' then daily_paid_adds_cnt else null end) as inter_paid_add,
sum(case when country_cd !='us' then daily_trial_adds_cnt else null end) as inter_trial_add,
sum(case when payment_method in ('china_pptv', 'osn', 'rogers', 'astro') then daily_new_adds_cnt else null end) as lp_adds,
sum(daily_new_adds_cnt) as adds,
sum(case when country_cd='us' and payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') then daily_paid_loss_cnt else null end) as domest_paid_loss,
sum(case when country_cd='us' and payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') then daily_trial_loss_cnt else null end) as domest_trial_loss,
sum(case when country_cd !='us' and payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') then daily_paid_loss_cnt else null end) as inter_paid_loss,
sum(case when country_cd !='us' and payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') then daily_trial_loss_cnt else null end) as inter_trial_loss,
sum(case when payment_method in ('china_pptv', 'osn', 'rogers', 'astro') then daily_loss_cnt else null end) as lp_loss,
sum(daily_loss_cnt) as loss,
sum(daily_tier2_prospect_loggedin_new_users_cnt) tier2_new_registrations,
sum(daily_unique_visitors_tier2_cnt) as Unique_Visitors_Daily_Prospects_inactives,
sum(daily_unique_visitors_tier3_cnt) as Unique_Visitors_Network_Subscribers_Daily      
from "entdwdb"."fds_nplus"."aggr_daily_subscription"
group by 1,2,3) a
left join
(select as_on_date-1 as date,dim_country_id,country_cd,
sum(case when country_cd='us' then total_paid_active_cnt else null end) as domest_paid_active,
sum(case when country_cd='us' then total_trial_active_cnt else null end) as domest_trial_active,
sum(case when country_cd !='us' then total_paid_active_cnt else null end) as inter_paid_active,
sum(case when country_cd !='us' then total_trial_active_cnt else null end) as inter_trial_active,
sum(case when payment_method in ('china_pptv', 'osn', 'rogers', 'astro') then total_active_cnt else null end) as lp_active,
sum(total_active_cnt) as active, 
sum(total_tier2_inactive_cnt) as inactive,
sum(total_tier2_inactive_dedup_cnt) as t2_inact_dedup,
sum(total_tier2_prospect_loggedin_dedup_cnt) as t2_inact_dedup_logged_in,
sum(total_tier2_prospect_loggedin_cnt) as Prospects_logged_in,
sum(total_tier2_prospect_nonlogged_cnt) as Prospects_not_logged_in,
sum(total_unique_visitors_tier2_mtd) as Unique_Visitors_MTD_Prospects_inactives,
sum(total_unique_visitors_tier3_mtd) as Unique_Visitors_Network_Subscribers_MTD 
from "entdwdb"."fds_nplus"."aggr_total_subscription" 
group by 1,2,3) b
on a.date = b.date
and a.dim_country_id = b.dim_country_id
and a.country_cd = b.country_cd
left join
(select * from cdm.dim_region_country where etl_source_name = 'Network 0') c
on a.dim_country_id = c.dim_country_id
order by date,dim_country_id,country_cd,country_nm