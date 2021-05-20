{{
  config({
		"materialized": 'ephemeral'
  })
}}
select
    a.*,
    case
        when b.email is null
        then '05. Non-Network User - Not present in database'
        when b.subscriber_tier_status = 'active'
        then '01. Network User - Active'
        when b.subscriber_tier_status in ('inactive',
                                          'apple_iap',
                                          'roku_iap',
                                          'google_iap',
                                          'unknown')
        then '02. Network User - Inactive'
        when b.subscriber_tier_status = 'prospect'
        then '03. Network User - Prospect'
        else '04. Non-Network User - Present in database'
    end as fan_status
from
    {{ref('intm_pii_customer_rpt_clean_tbl')}} a
left join
    {{source('fds_pii','fc_fan_variables_consolidated')}} b
on
    lower(trim(a.email)) = lower(trim(b.email))