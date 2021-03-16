
with dbt__CTE__INTERNAL_test as (

select distinct query1 from
(select case when total>0 then null else 1 end query1 from
(select 'comp_ppvs' as metric_nm, 'n/a' as dm_dimension_val, count(event_name) as total from 
udl_nplus.raw_da_weekly_ppv_hourly_comps 
where event_date >= current_date and 
(comp1_event_name != '' or comp2_event_name != '' or comp3_event_name != ''))) where query1 is not null
)select count(*) from dbt__CTE__INTERNAL_test