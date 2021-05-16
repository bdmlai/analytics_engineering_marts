{{
  config({
		"schema": 'dt_prod_support',
		"materialized": 'table','tags': "rpt_nplus_daily_live_streams", 
		'post-hook': 'grant select on {{ this }} to public'
  })
}}
select a.external_id,a.time_interval,a.prev_time_interval,
        (b.curr_cnt-a.prev_cnt) as users_added
  from  {{ref('intm_nplus_fans_in_both_section_live')}} a
  join  {{ref('intm_nplus_count_all_fans_first_section_live')}} b on a.external_id=b.external_id
   and a.time_interval=b.time_interval
   and a.prev_time_interval=b.prev_time_interval