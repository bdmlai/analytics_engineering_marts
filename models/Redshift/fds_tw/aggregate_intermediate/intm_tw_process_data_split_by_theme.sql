{{
  config({
		"materialized": 'ephemeral'
  })
}}
with cte as 
(select n::int from
  (select row_number() over (order by true) as n from {{ref('intm_tw_process_data_events')}})
cross join
(select  max(regexp_count(theme, '[&]')) as max_num from {{ref('intm_tw_process_data_events')}}) where n <= max_num + 1)
select distinct show_date
        ,show_name
        ,segment_name        
        ,link  
        ,reaction_speculation
        ,trim(SPLIT_PART(theme,'&',n)) as theme
 from {{ref('intm_tw_process_data_events')}}
cross join cte
where  split_part(theme,'&',n) is not null and split_part(theme,'&',n) != ''
