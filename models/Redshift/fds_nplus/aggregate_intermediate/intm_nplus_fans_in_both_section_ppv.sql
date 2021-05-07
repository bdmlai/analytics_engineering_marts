{{
  config({
		"materialized": 'ephemeral'
  })
}}
select count(src_fan_id) as prev_cnt,external_id,time_interval,prev_time_interval 
 from
 (
 select distinct external_id,src_fan_id,time_interval,prev_time_interval from {{ref('intm_nplus_fans_first_section_ppv')}}
 intersect
 select distinct external_id,src_fan_id,time_interval,prev_time_interval from {{ref('intm_nplus_fans_second_section_ppv')}} 
 )
 group by external_id,time_interval,prev_time_interval