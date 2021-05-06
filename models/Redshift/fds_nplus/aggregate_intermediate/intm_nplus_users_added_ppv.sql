{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.external_id,a.time_interval,a.prev_time_interval,
        (b.curr_cnt-a.prev_cnt) as users_added
  from  {{ref('intm_nplus_fans_in_both_section_ppv')}} a
  join  {{ref('intm_nplus_count_all_fans_first_section_ppv')}} b on a.external_id=b.external_id
   and a.time_interval=b.time_interval
   and a.prev_time_interval=b.prev_time_interval