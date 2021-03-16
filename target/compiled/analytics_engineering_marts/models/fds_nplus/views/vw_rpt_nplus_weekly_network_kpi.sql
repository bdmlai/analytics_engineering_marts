

SELECT a.date,a.country_cd,a.country_nm,
a.daily_paid_adds_cnt_new,
a.daily_paid_adds_cnt_winback,
a.daily_paid_loss_cnt_new,
a.daily_paid_loss_cnt_winback,
a.lp_loss,
a.loss,
a.lp_adds,
a.adds,
a.tier2_new_registrations,
a.unique_visitors_daily_prospects_inactives,
a.unique_visitors_network_subscribers_daily,
a.domest_paid_active,
a.domest_trial_active,
a.inter_paid_active,
a.inter_trial_active,
a.lp_active,
a.t2_inact_dedup,
a.t2_inact_dedup_logged_in,
b.t2_prosp_dedup_logged_in_da,
b.total_unique_visitors_tier2_inactive_mtd,
b.total_unique_visitors_tier2_prospect_mtd,
b.dice_iap_active,
b.apple_iap_active,
b.roku_iap_active,
b.google_iap_active,
b.total_prospects_ever,
b.total_prospects_ever - b.t2_prosp_dedup_logged_in_da as prospects_not_logged_in,
c.new_to_t3,
c.prospect_upgrades,
c.daily_vol_loss_cnt,
c.daily_invol_loss_cnt,
c.new_adds
FROM "entdwdb"."fds_nplus"."vw_rpt_network_daily_subscription_kpis" a

left join
        (select as_on_date-1 as total_date,dim_country_id as ctry_id,country_cd as ctry_cd,
        sum(total_tier2_true_prospect_loggedin_last12m_cnt) as t2_prosp_dedup_logged_in_da,
        sum(total_unique_visitors_tier2_inactive_mtd) as total_unique_visitors_tier2_inactive_mtd, 
        (sum(total_unique_visitors_tier2_mtd)-sum(total_unique_visitors_tier2_inactive_mtd)) as total_unique_visitors_tier2_prospect_mtd,
        sum(case when payment_method not in ('google_iap','china_pptv', 'osn', 'rogers', 'astro','roku_iap','apple_iap') 
                 then total_paid_active_cnt else null end) as dice_iap_active,
        sum(case when payment_method in ('apple_iap') then total_paid_active_cnt else null end) as apple_iap_active,
        sum(case when payment_method in ('roku_iap') then total_paid_active_cnt else null end) as roku_iap_active,
        sum(case when payment_method in ('google_iap') then total_paid_active_cnt else null end) as google_iap_active,
        sum(total_tier2_true_prospect_cnt) as total_prospects_ever
        from "entdwdb"."fds_nplus"."aggr_total_subscription"       
        group by 1,2,3) b
on a.date = b.total_date
and a.dim_country_id = b.ctry_id
and a.country_cd = b.ctry_cd

left join 
        (select as_on_date -1 as daily_date,dim_country_id as ctr_id,country_cd as ctr_cd,
        (sum(case when order_type='first' and payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') 
                  then daily_new_adds_cnt else null end)-sum(first_total_adds_upgrades)) as new_to_t3,
         sum(first_total_adds_upgrades) as prospect_upgrades,
         sum(case when payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') 
                  then daily_vol_loss_cnt else null end) as daily_vol_loss_cnt,
         sum(case when payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') 
                  then daily_invol_loss_cnt else null end) as daily_invol_loss_cnt,
         sum(case when payment_method not in ('china_pptv', 'osn', 'rogers', 'astro') 
                  then daily_new_adds_cnt else null end) as new_adds
         from "entdwdb"."fds_nplus"."aggr_daily_subscription"        
         group by 1,2,3) c
on a.date = c.daily_date
and a.dim_country_id = c.ctr_id
and a.country_cd = c.ctr_cd
where a.date >='2017-01-01'