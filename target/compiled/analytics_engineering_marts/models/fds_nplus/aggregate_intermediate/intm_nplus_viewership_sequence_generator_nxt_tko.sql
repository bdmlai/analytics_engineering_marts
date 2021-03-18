

with digit as (
    select 0 as d union all 
    select 1 union all select 2 union all select 3 union all
    select 4 union all select 5 union all select 6 union all
    select 7 union all select 8 union all select 9        
),
seq as (
    select a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) + (10000 * e.d)+ (100000 * f.d) as num
    from digit a
        cross join
        digit b
        cross join
        digit c
        cross join
        digit d
        cross join
        digit e
        cross join
        digit f
        
    order by 1        
),

#series as 
(select num from seq where num <= datediff('m', ((select substring(getdate()-7,1,11)||'00:00:02')::TIMESTAMP ), (getdate()::TIMESTAMP))/5+1),

inter AS 
        (select
        (date_trunc('hour', getdate()) 
        + date_part('minute', getdate())::int / 5 * interval '5 min' - (num * interval '5 min')) + interval '2 sec'
		as intvl_dttm
        FROM #series 
        ),
       
       
llog AS (
        select premiere_date,external_id,title,segmenttype,content_duration, seg_num, milestone,matchtype,
		talentactions,move,finishtype,additionaltalent, venuelocation,venuename,state,   
		in_time_est :: timestamp as begindate, out_time_est :: timestamp as enddate,
         (lead(in_time_est, 1) OVER (partition by external_id order by in_time_est asc )) as nxt_seg_begindate
        from __dbt__CTE__intm_nplus_event_litelog_nxt_tko
        ),
 
 #T8 as 		
(SELECT *
, CASE WHEN begindate between intvl_dttm and intvl_dttm + interval '5 mins' then begindate
        WHEN enddate between  intvl_dttm - interval '5 mins' and intvl_dttm then enddate
        ELSE intvl_dttm
  END AS time_interval
FROM llog
CROSS JOIN
inter
WHERE 
(inter.intvl_dttm between (llog.begindate - interval '5 min') and (llog.enddate + interval '5 min'))
union all
SELECT *, intvl_dttm AS time_interval
FROM llog
CROSS JOIN
inter
WHERE (inter.intvl_dttm > llog.enddate and inter.intvl_dttm < llog.nxt_seg_begindate)),

#T9 as 
( select *, (lag(time_interval, 1) OVER (partition by external_id order by time_interval asc )) as prev_time_interval
        from #T8)
 select * from #T9