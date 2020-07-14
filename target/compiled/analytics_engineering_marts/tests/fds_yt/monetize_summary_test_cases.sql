

-- If channel_name doesn't have any channels with UpUpDownDown The Bella Twins WWE (failure).
-- If channel_name  have any channels with UpUpDownDown The Bella Twins WWE (success)

--script to check the three channes exists or not

with channel_check as (select COUNT(distinct channel_name) cnt from   dwh_read_write.agg_yt_monetization_summary  where view_date=trunc(current_date)-1
and  channel_name  
in ('UpUpDownDown','The Bella Twins','WWE') having count(distinct channel_name)<3),
check_data as (select count(*) from dwh_read_write.agg_yt_monetization_summary where view_date=trunc(current_date)-1
having count(*)<=0)
select * from channel_check
union 
select * from check_data