{{
  config({
		"materialized": 'ephemeral'
  })
}}


with intm_le_upcoming_events as
(
select state_nm,country_nm,a.dim_event_id,event_date, event_src_sys_id,venue_city,c.venue_nm,a.brand_nm,a.event_Type_cd,event_nm,
(event_date- (current_date)) as days_to_event,les_event_capacity as event_capacity,les_gross_amt as gross,
les_total_paid as paid,les_total_paid/nullif(les_event_capacity,0) as util 
from {{source('fds_le','fact_combined_ticket_sale')}}  a
left join {{source('cdm','dim_event')}} b on a.dim_event_id=b.dim_event_id
left join {{source('cdm','dim_venue')}}  c on a.dim_venue_id=c.dim_venue_id
where  event_status = 'Published' and event_date >current_date
),
intm_le_bestcomp_ppv as 
(
select state_nm,country_nm,dim_event_id,event_date as beventdate, venue_city,venue_nm as bvenuename,
brand_nm,event_Type_cd,
event_capacity as bcapacity, gross as bgross,paid as bpaid,util as butil,ranking,event_nm 
from (
		select state_nm,country_nm,a.dim_event_id,event_date, venue_city,c.venue_nm,a.brand_nm,a.event_Type_cd,
les_event_capacity as event_capacity,les_gross_amt as gross,event_Date-current_Date as days_to_event,event_nm,
les_total_paid as paid,les_total_paid/nullif(les_event_capacity,0) as util ,
rank() over (partition by event_nm order by event_date desc) AS RANKING
from {{source('fds_le','fact_combined_ticket_sale')}}  a
left join {{source('cdm','dim_event')}}b on a.dim_event_id=b.dim_event_id
left join {{source('cdm','dim_venue')}}  c on a.dim_venue_id=c.dim_venue_id
 where event_date >= '2012-01-01' and event_status = 'Published'
and  (event_nm) in 
        (select distinct event_nm from intm_le_upcoming_events  where event_type_cd = 'PPV') 
)where ranking = 2

		),
		
		
		intm_le_bests_ppv as
		(
	select min(a.days_to_event) as days_to_event,max(a.gross_amt) as bsgross,max(a.paid) as bspaid,
max(a.util) as bsutil,b.dim_event_id from	
	(select a.dim_event_id,city_nm,a.brand_nm,event_type_CD,a.event_dttm :: date - record_dttm :: date as
  days_to_event,event_nm,gross_amt,paid,
 paid/capacity as util
from {{source('fds_le','fact_ticket_self_reported')}} a
left join {{source('cdm','dim_venue')}} b on a.dim_venue_id=b.dim_venue_id
left join {{source('cdm','dim_event')}} c on a.dim_event_id=c.dim_event_id
where a.dim_event_id in (select distinct dim_event_id from intm_le_bestcomp_ppv))a
left join intm_le_upcoming_events b on a.event_nm = b.event_nm and b.event_type_cd = 'PPV' and 
a.days_to_event >= b.days_to_event 
group by 5
)
,

intm_le_avg_ppv as
(
select event_nm,avg(gross) as agross,avg(paid) as apaid, avg(event_capacity) as acapacity,
sum(paid)/sum(event_capacity) as autil,count(event_date) as ano_event from
 (
		select state_nm,country_nm,a.dim_event_id,event_date, venue_city,c.venue_nm,a.brand_nm,a.event_Type_cd,
les_event_capacity as event_capacity,les_gross_amt as gross,event_Date-current_Date as days_to_event,event_nm,
les_total_paid as paid,les_total_paid/nullif(les_event_capacity,0) as util ,
rank() over (partition by event_nm order by event_date desc) AS RANKING
from {{source('fds_le','fact_combined_ticket_sale')}}  a
left join {{source('cdm','dim_event')}} b on a.dim_event_id=b.dim_event_id
left join {{source('cdm','dim_venue')}} c on a.dim_venue_id=c.dim_venue_id
 where event_date >= '2016-01-01' and event_status = 'Published'
and  (event_nm) in 
        (select distinct event_nm from intm_le_upcoming_events where event_type_cd = 'PPV' ) 
)where ranking >= 2
group by 1
),

 intm_le_avgcomp_ppv as
(		
 select dim_event_id from (
select state_nm,country_nm,a.dim_event_id,event_date, event_src_sys_id,venue_city,c.venue_nm,a.brand_nm,a.event_Type_cd,event_nm,
rank() over (partition by event_nm order by event_date desc) AS RANKING
from {{source('fds_le','fact_combined_ticket_sale')}}  a
left join {{source('cdm','dim_event')}} b on a.dim_event_id=b.dim_event_id
left join {{source('cdm','dim_venue')}}  c on a.dim_venue_id=c.dim_venue_id
 where event_date > '2016-01-01'  and event_status = 'Published'
and (event_nm) in 
        (select distinct event_nm from intm_le_upcoming_events where event_type_cd = 'PPV' ) 
)where ranking >= 2
),


intm_le_avgs_ppv as
(
select dim_event_id, avg(gross) as asgross,avg(paid) as aspaid, avg(capacity) as ascapacity,sum(paid)/sum(capacity) as asutil,
count(compjde) as asno_event
 from (
        select min(a.days_to_event) as days_to_event,max(a.gross_amt) as gross,max(a.paid) as paid ,max(a.capacity) as capacity,
		a.dim_event_id as compjde, b.dim_event_id from
                (select a.dim_event_id,city_nm,a.brand_nm,event_type_cd,a.event_dttm :: date - record_dttm :: date as days_to_event,
				gross_amt,paid,capacity,event_nm from {{source('fds_le','fact_ticket_self_reported')}} a
left join {{source('cdm','dim_venue')}}  b on a.dim_venue_id=b.dim_venue_id
left join {{source('cdm','dim_event')}} c on a.dim_event_id=c.dim_event_id
				where a.dim_event_id in (select * from intm_le_avgcomp_ppv)) as a
               left join intm_le_upcoming_events b on a.event_nm = b.event_nm and b.event_type_cd = 'PPV' and 
a.days_to_event >= b.days_to_event 
group by 5,6
)where dim_event_id is not null group by 1
)


 select a.venue_city,a.venue_nm,a.brand_nm,a.event_Type_cd,a.event_date,a.days_to_event,a.event_capacity,a.event_nm,
gross,paid,util,bvenuename, beventdate,bcapacity,bgross,bpaid, butil,
acapacity,agross,apaid,autil,ano_event ,bsgross,bspaid,bsutil,
ascapacity,asgross, aspaid,asutil,asno_event
 from intm_le_upcoming_events a 
left join intm_le_bestcomp_PPV b on a.event_nm = b.event_nm
left join intm_le_avg_ppv c on a.event_nm = b.event_nm
left join intm_le_bests_ppv d on a.dim_event_id = d.dim_event_id
left join intm_le_avgs_ppv e on a.dim_event_id = e.dim_event_id
where a.event_type_cd = 'PPV'