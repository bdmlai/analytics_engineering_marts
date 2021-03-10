 {{
  config({
	"schema": 'fds_le',	
	"materialized": 'incremental',
	"pre-hook": ['delete from fds_le.rpt_le_weekly_consolidated_kpi;',
	"--create dates for rollup
drop table if exists #dim_dates;
create table #dim_dates as
select distinct cal_year, --extract('month' from cal_year_mon_week_begin_date) as cal_mth_num,
case when cal_year = extract('year' from cal_year_mon_week_begin_date) then extract('month' from cal_year_mon_week_begin_date)
     when cal_year = extract('year' from cal_year_mon_week_end_date) then extract('month' from cal_year_mon_week_end_date)   
     end as cal_mth_num, 
case when cal_year_week_num_mon is null then 1 else cal_year_week_num_mon end as cal_year_week_num_mon,
cal_year_mon_week_begin_date, cal_year_mon_week_end_date
from cdm.dim_date where cal_year_mon_week_begin_date >= trunc(dateadd('year',-1,date_trunc('year',getdate()))) 
and cal_year_mon_week_end_date < date_trunc('week',getdate());

--create live events weekly dataset
drop table if exists #dp_wkly;
create table #dp_wkly as
select b.*,a.no_of_total_events_wk::decimal(15,1),
a.no_of_house_events_wk::decimal(15,1),
a.no_of_raw_house_events_wk::decimal(15,1),
a.no_of_smackdown_house_events_wk::decimal(15,1),
a.no_of_combined_house_events_wk::decimal(15,1),
a.no_of_tv_events_wk::decimal(15,1),
a.no_of_raw_tv_events_wk::decimal(15,1),
a.no_of_smackdown_tv_events_wk::decimal(15,1),
a.no_of_combined_tv_events_wk::decimal(15,1),
a.no_of_ppv_events_wk::decimal(15,1),
a.total_paid_attendance_wk::decimal(15,1),
a.capacity_wk::decimal(15,1),
a.total_paid_utilization_wk::decimal(15,1),
a.avg_total_attendance_wk::decimal(15,1),
a.avg_house_event_attendance_wk::decimal(15,1),
a.avg_raw_house_event_attendance_wk::decimal(15,1),
a.avg_smackdown_house_event_attendance_wk::decimal(15,1),
a.avg_cmb_house_event_attendance_wk::decimal(15,1),
a.avg_tv_event_attendance_wk::decimal(15,1),
a.avg_raw_tv_event_attendance_wk::decimal(15,1),
a.avg_smackdown_tv_event_attendance_wk::decimal(15,1),
a.avg_cmb_tv_event_attendance_wk::decimal(15,1),
a.avg_ppv_event_attendance_wk::decimal(15,1),
'Live Events' as platform
from 
#dim_dates b
left join 
(       
select date_trunc('week',event_date) as monday_date,
count(dim_event_id) as no_of_total_events_wk,
count(case when event_type_cd = 'LE' then dim_event_id else null end) as no_of_house_events_wk,
count(case when event_type_cd = 'LE' and brand_nm = 'RAW' then dim_event_id else null end) as no_of_raw_house_events_wk,
count(case when event_type_cd = 'LE' and brand_nm = 'SMD' then dim_event_id else null end) as no_of_smackdown_house_events_wk,
count(case when event_type_cd = 'LE' and brand_nm = 'CMB' then dim_event_id else null end) as no_of_combined_house_events_wk,
count(case when event_type_cd = 'TV' then dim_event_id else null end) as no_of_tv_events_wk,
count(case when event_type_cd = 'TV' and brand_nm = 'RAW' then dim_event_id else null end) as no_of_raw_tv_events_wk,
count(case when event_type_cd = 'TV' and brand_nm = 'SMD' then dim_event_id else null end) as no_of_smackdown_tv_events_wk,
count(case when event_type_cd = 'TV' and brand_nm = 'CMB' then dim_event_id else null end) as no_of_combined_tv_events_wk,
count(case when event_type_cd = 'PPV' then dim_event_id else null end) as no_of_ppv_events_wk,
sum(les_total_paid) as total_paid_attendance_wk,
sum(les_event_capacity) as capacity_wk,
sum(les_total_paid)::decimal(15,1)/NULLIF(sum(les_event_capacity)::decimal(15,1),0) as total_paid_utilization_wk,
avg(les_total_paid+les_total_comp) as  avg_total_attendance_wk,
avg(case when event_type_cd = 'LE' then les_total_paid+les_total_comp else null end) as avg_house_event_attendance_wk,
avg(case when event_type_cd = 'LE' and brand_nm = 'RAW' then les_total_paid+les_total_comp else null end) as avg_raw_house_event_attendance_wk,
avg(case when event_type_cd = 'LE' and brand_nm = 'SMD' then les_total_paid+les_total_comp else null end) as avg_smackdown_house_event_attendance_wk,
avg(case when event_type_cd = 'LE' and brand_nm = 'CMB' then les_total_paid+les_total_comp else null end) as avg_cmb_house_event_attendance_wk,
avg(case when event_type_cd = 'TV' then les_total_paid+les_total_comp else null end) as avg_tv_event_attendance_wk,
avg(case when event_type_cd = 'TV' and brand_nm = 'RAW' then les_total_paid+les_total_comp else null end) as avg_raw_tv_event_attendance_wk,
avg(case when event_type_cd = 'TV' and brand_nm = 'SMD' then les_total_paid+les_total_comp else null end) as avg_smackdown_tv_event_attendance_wk,
avg(case when event_type_cd = 'TV' and brand_nm = 'CMB' then les_total_paid+les_total_comp else null end) as avg_cmb_tv_event_attendance_wk,
avg(case when event_type_cd = 'PPV' then les_total_paid+les_total_comp else null end) as avg_ppv_event_attendance_wk
from fds_le.fact_combined_ticket_sale
where event_date >= trunc(dateadd('year',-1,date_trunc('year',getdate())))
and brand_nm!='NXT' and country_nm in ('united states','canada')
group by 1
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date;

drop table if exists #dp_wkly1;
create table #dp_wkly1 as
select a.*, a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week,
b.cal_year as prev_cal_year, b.cal_year_week_num_mon as prev_cal_year_week_num_mon,
b.cal_year_mon_week_begin_date as prev_cal_year_mon_week_begin_date, b.cal_year_mon_week_end_date as prev_cal_year_mon_week_end_date,

coalesce(b.no_of_total_events_wk,0) as prev_no_of_total_events_wk, 
coalesce(b.no_of_house_events_wk,0) as prev_no_of_house_events_wk,
coalesce(b.no_of_raw_house_events_wk,0) as prev_no_of_raw_house_events_wk,
coalesce(b.no_of_smackdown_house_events_wk,0) as prev_no_of_smackdown_house_events_wk,
coalesce(b.no_of_combined_house_events_wk,0) as prev_no_of_combined_house_events_wk,
coalesce(b.no_of_tv_events_wk,0) as prev_no_of_tv_events_wk,
coalesce(b.no_of_raw_tv_events_wk,0) as prev_no_of_raw_tv_events_wk,
coalesce(b.no_of_smackdown_tv_events_wk,0) as prev_no_of_smackdown_tv_events_wk,
coalesce(b.no_of_combined_tv_events_wk,0) as prev_no_of_combined_tv_events_wk,
coalesce(b.no_of_ppv_events_wk,0) as prev_no_of_ppv_events_wk,
coalesce(b.total_paid_attendance_wk,0) as prev_total_paid_attendance_wk,
coalesce(b.capacity_wk,0) as prev_capacity_wk,
coalesce(b.total_paid_utilization_wk,0) as prev_total_paid_utilization_wk,
coalesce(b.avg_total_attendance_wk,0) as prev_avg_total_attendance_wk,
coalesce(b.avg_house_event_attendance_wk,0) as prev_avg_house_event_attendance_wk,
coalesce(b.avg_raw_house_event_attendance_wk,0) as prev_avg_raw_house_event_attendance_wk, 
coalesce(b.avg_smackdown_house_event_attendance_wk,0) as prev_avg_smackdown_house_event_attendance_wk,
coalesce(b.avg_cmb_house_event_attendance_wk,0) as prev_avg_cmb_house_event_attendance_wk,
coalesce(b.avg_tv_event_attendance_wk,0) as prev_avg_tv_event_attendance_wk,
coalesce(b.avg_raw_tv_event_attendance_wk,0) as prev_avg_raw_tv_event_attendance_wk,
coalesce(b.avg_smackdown_tv_event_attendance_wk,0) as prev_avg_smackdown_tv_event_attendance_wk,
coalesce(b.avg_cmb_tv_event_attendance_wk,0) as prev_avg_cmb_tv_event_attendance_wk,
coalesce(b.avg_ppv_event_attendance_wk,0) as prev_avg_ppv_event_attendance_wk
from 
#dp_wkly a
left join 
#dp_wkly b
on (a.cal_year-1) = b.cal_year and a.cal_year_week_num_mon = b.cal_year_week_num_mon;

--create monthly dataset
drop table if exists #dp_mthly;
create table #dp_mthly as
select a.platform, a.cal_year,a.cal_mth_num, a.cal_year_week_num_mon, a.cal_year_mon_week_begin_date, a.cal_year_mon_week_end_date,
a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week, 
a.prev_cal_year, a.prev_cal_year_week_num_mon,
a.prev_cal_year_mon_week_begin_date, a.prev_cal_year_mon_week_end_date,

sum(b.no_of_total_events_wk) as no_of_total_events_mtd, 
sum(b.no_of_house_events_wk) as no_of_house_events_mtd,
sum(b.no_of_raw_house_events_wk) as no_of_raw_house_events_mtd,
sum(b.no_of_smackdown_house_events_wk) as no_of_smackdown_house_events_mtd,
sum(b.no_of_combined_house_events_wk) as no_of_combined_house_events_mtd,
sum(b.no_of_tv_events_wk) as no_of_tv_events_mtd,
sum(b.no_of_raw_tv_events_wk) as no_of_raw_tv_events_mtd,
sum(b.no_of_smackdown_tv_events_wk) as no_of_smackdown_tv_events_mtd,
sum(b.no_of_combined_tv_events_wk) as no_of_combined_tv_events_mtd,
sum(b.no_of_ppv_events_wk) as no_of_ppv_events_mtd,
sum(b.total_paid_attendance_wk) as total_paid_attendance_mtd,
sum(b.capacity_wk) as capacity_mtd,
avg(b.total_paid_utilization_wk) as total_paid_utilization_mtd,
avg(b.avg_total_attendance_wk) as avg_total_attendance_mtd,
avg(b.avg_house_event_attendance_wk) as avg_house_event_attendance_mtd,
avg(b.avg_raw_house_event_attendance_wk) as avg_raw_house_event_attendance_mtd, 
avg(b.avg_smackdown_house_event_attendance_wk) as avg_smackdown_house_event_attendance_mtd,
avg(b.avg_cmb_house_event_attendance_wk) as avg_cmb_house_event_attendance_mtd,
avg(b.avg_tv_event_attendance_wk) as avg_tv_event_attendance_mtd,
avg(b.avg_raw_tv_event_attendance_wk) as avg_raw_tv_event_attendance_mtd,
avg(b.avg_smackdown_tv_event_attendance_wk) as avg_smackdown_tv_event_attendance_mtd,
avg(b.avg_cmb_tv_event_attendance_wk) as avg_cmb_tv_event_attendance_mtd,
avg(b.avg_ppv_event_attendance_wk) as avg_ppv_event_attendance_mtd,

sum(b.prev_no_of_total_events_wk) as prev_no_of_total_events_mtd, 
sum(b.prev_no_of_house_events_wk) as prev_no_of_house_events_mtd,
sum(b.prev_no_of_raw_house_events_wk) as prev_no_of_raw_house_events_mtd,
sum(b.prev_no_of_smackdown_house_events_wk) as prev_no_of_smackdown_house_events_mtd,
sum(b.prev_no_of_combined_house_events_wk) as prev_no_of_combined_house_events_mtd,
sum(b.prev_no_of_tv_events_wk) as prev_no_of_tv_events_mtd,
sum(b.prev_no_of_raw_tv_events_wk) as prev_no_of_raw_tv_events_mtd,
sum(b.prev_no_of_smackdown_tv_events_wk) as prev_no_of_smackdown_tv_events_mtd,
sum(b.prev_no_of_combined_tv_events_wk) as prev_no_of_combined_tv_events_mtd,
sum(b.prev_no_of_ppv_events_wk) as prev_no_of_ppv_events_mtd,
sum(b.prev_total_paid_attendance_wk) as prev_total_paid_attendance_mtd,
sum(b.prev_capacity_wk) as prev_capacity_mtd,
avg(b.prev_total_paid_utilization_wk) as prev_total_paid_utilization_mtd,
avg(b.prev_avg_total_attendance_wk) as prev_avg_total_attendance_mtd,
avg(b.prev_avg_house_event_attendance_wk) as prev_avg_house_event_attendance_mtd,
avg(b.prev_avg_raw_house_event_attendance_wk) as prev_avg_raw_house_event_attendance_mtd, 
avg(b.prev_avg_smackdown_house_event_attendance_wk) as prev_avg_smackdown_house_event_attendance_mtd,
avg(b.prev_avg_cmb_house_event_attendance_wk) as prev_avg_cmb_house_event_attendance_mtd,
avg(b.prev_avg_tv_event_attendance_wk) as prev_avg_tv_event_attendance_mtd,
avg(b.prev_avg_raw_tv_event_attendance_wk) as prev_avg_raw_tv_event_attendance_mtd,
avg(b.prev_avg_smackdown_tv_event_attendance_wk) as prev_avg_smackdown_tv_event_attendance_mtd,
avg(b.prev_avg_cmb_tv_event_attendance_wk) as prev_avg_cmb_tv_event_attendance_mtd,
avg(b.prev_avg_ppv_event_attendance_wk) as prev_avg_ppv_event_attendance_mtd
from #dp_wkly1 a
left join #dp_wkly1 b
on a.cal_year = b.cal_year and a.cal_mth_num = b.cal_mth_num and a.cal_year_week_num_mon >= b.cal_year_week_num_mon 
group by 1,2,3,4,5,6,7,8,9,10,11;

--create yearly dataset
drop table if exists #dp_yrly;
create table #dp_yrly as
select a.platform,a.cal_year,a.cal_mth_num, a.cal_year_week_num_mon, a.cal_year_mon_week_begin_date, a.cal_year_mon_week_end_date,
a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week,
a.prev_cal_year, a.prev_cal_year_week_num_mon,
a.prev_cal_year_mon_week_begin_date, a.prev_cal_year_mon_week_end_date,

sum(b.no_of_total_events_wk) as no_of_total_events_ytd, 
sum(b.no_of_house_events_wk) as no_of_house_events_ytd,
sum(b.no_of_raw_house_events_wk) as no_of_raw_house_events_ytd,
sum(b.no_of_smackdown_house_events_wk) as no_of_smackdown_house_events_ytd,
sum(b.no_of_combined_house_events_wk) as no_of_combined_house_events_ytd,
sum(b.no_of_tv_events_wk) as no_of_tv_events_ytd,
sum(b.no_of_raw_tv_events_wk) as no_of_raw_tv_events_ytd,
sum(b.no_of_smackdown_tv_events_wk) as no_of_smackdown_tv_events_ytd,
sum(b.no_of_combined_tv_events_wk) as no_of_combined_tv_events_ytd,
sum(b.no_of_ppv_events_wk) as no_of_ppv_events_ytd,
sum(b.total_paid_attendance_wk) as total_paid_attendance_ytd,
sum(b.capacity_wk) as capacity_ytd,
avg(b.total_paid_utilization_wk) as total_paid_utilization_ytd,
avg(b.avg_total_attendance_wk) as avg_total_attendance_ytd,
avg(b.avg_house_event_attendance_wk) as avg_house_event_attendance_ytd,
avg(b.avg_raw_house_event_attendance_wk) as avg_raw_house_event_attendance_ytd, 
avg(b.avg_smackdown_house_event_attendance_wk) as avg_smackdown_house_event_attendance_ytd,
avg(b.avg_cmb_house_event_attendance_wk) as avg_cmb_house_event_attendance_ytd,
avg(b.avg_tv_event_attendance_wk) as avg_tv_event_attendance_ytd,
avg(b.avg_raw_tv_event_attendance_wk) as avg_raw_tv_event_attendance_ytd,
avg(b.avg_smackdown_tv_event_attendance_wk) as avg_smackdown_tv_event_attendance_ytd,
avg(b.avg_cmb_tv_event_attendance_wk) as avg_cmb_tv_event_attendance_ytd,
avg(b.avg_ppv_event_attendance_wk) as avg_ppv_event_attendance_ytd,

sum(b.prev_no_of_total_events_wk) as prev_no_of_total_events_ytd, 
sum(b.prev_no_of_house_events_wk) as prev_no_of_house_events_ytd,
sum(b.prev_no_of_raw_house_events_wk) as prev_no_of_raw_house_events_ytd,
sum(b.prev_no_of_smackdown_house_events_wk) as prev_no_of_smackdown_house_events_ytd,
sum(b.prev_no_of_combined_house_events_wk) as prev_no_of_combined_house_events_ytd,
sum(b.prev_no_of_tv_events_wk) as prev_no_of_tv_events_ytd,
sum(b.prev_no_of_raw_tv_events_wk) as prev_no_of_raw_tv_events_ytd,
sum(b.prev_no_of_smackdown_tv_events_wk) as prev_no_of_smackdown_tv_events_ytd,
sum(b.prev_no_of_combined_tv_events_wk) as prev_no_of_combined_tv_events_ytd,
sum(b.prev_no_of_ppv_events_wk) as prev_no_of_ppv_events_ytd,
sum(b.prev_total_paid_attendance_wk) as prev_total_paid_attendance_ytd,
sum(b.prev_capacity_wk) as prev_capacity_ytd,
avg(b.prev_total_paid_utilization_wk) as prev_total_paid_utilization_ytd,
avg(b.prev_avg_total_attendance_wk) as prev_avg_total_attendance_ytd,
avg(b.prev_avg_house_event_attendance_wk) as prev_avg_house_event_attendance_ytd,
avg(b.prev_avg_raw_house_event_attendance_wk) as prev_avg_raw_house_event_attendance_ytd, 
avg(b.prev_avg_smackdown_house_event_attendance_wk) as prev_avg_smackdown_house_event_attendance_ytd,
avg(b.prev_avg_cmb_house_event_attendance_wk) as prev_avg_cmb_house_event_attendance_ytd,
avg(b.prev_avg_tv_event_attendance_wk) as prev_avg_tv_event_attendance_ytd,
avg(b.prev_avg_raw_tv_event_attendance_wk) as prev_avg_raw_tv_event_attendance_ytd,
avg(b.prev_avg_smackdown_tv_event_attendance_wk) as prev_avg_smackdown_tv_event_attendance_ytd,
avg(b.prev_avg_cmb_tv_event_attendance_wk) as prev_avg_cmb_tv_event_attendance_ytd,
avg(b.prev_avg_ppv_event_attendance_wk) as prev_avg_ppv_event_attendance_ytd
from #dp_wkly1 a
left join #dp_wkly1 b
on a.cal_year = b.cal_year and a.cal_year_week_num_mon >= b.cal_year_week_num_mon 
group by 1,2,3,4,5,6,7,8,9,10,11;

--pivot weekly dataset
drop table if exists #dp_wkly_pivot;
create table #dp_wkly_pivot as
select * from
(
select 'Weekly' as granularity, platform, 'Number of Total Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_total_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_total_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_house_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_house_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of Raw House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_raw_house_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_raw_house_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of Smackdown House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_smackdown_house_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_smackdown_house_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of Combined House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_combined_house_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_combined_house_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_tv_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_tv_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of Raw TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_raw_tv_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_raw_tv_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of Smackdown TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_smackdown_tv_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_smackdown_tv_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of Combined TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_combined_tv_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_combined_tv_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Number of PPV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_ppv_events_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_ppv_events_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Total Paid Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_total_paid_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Capacity' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, capacity_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_capacity_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Total Paid Utilization' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_utilization_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_total_paid_utilization_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Total Paid Utilization New' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_attendance_wk/nullif(capacity_wk,0) as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, 
prev_total_paid_attendance_wk/nullif(prev_capacity_wk,0) as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average Total Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_total_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_total_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_house_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_house_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average Raw House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_raw_house_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_raw_house_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average Smackdown House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_smackdown_house_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_smackdown_house_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average CMB House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_cmb_house_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_cmb_house_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_tv_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_tv_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average Raw TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_raw_tv_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_raw_tv_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average Smackdown TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_smackdown_tv_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_smackdown_tv_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average CMB TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_cmb_tv_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_cmb_tv_event_attendance_wk as prev_year_value
from #dp_wkly1
union all
select 'Weekly' as granularity, platform, 'Average PPV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_ppv_event_attendance_wk as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_ppv_event_attendance_wk as prev_year_value
from #dp_wkly1
);

--pivot monthly dataset
drop table if exists #dp_mthly_pivot;
create table #dp_mthly_pivot as
select * from
(
select 'MTD' as granularity, platform, 'Number of Total Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_total_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_total_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_house_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_house_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of Raw House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_raw_house_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_raw_house_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of Smackdown House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_smackdown_house_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_smackdown_house_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of Combined House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_combined_house_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_combined_house_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_tv_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_tv_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of Raw TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_raw_tv_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_raw_tv_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of Smackdown TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_smackdown_tv_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_smackdown_tv_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of Combined TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_combined_tv_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_combined_tv_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Number of PPV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_ppv_events_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_ppv_events_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Total Paid Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_total_paid_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Capacity' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, capacity_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_capacity_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Total Paid Utilization' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_utilization_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_total_paid_utilization_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Total Paid Utilization New' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_attendance_mtd/nullif(capacity_mtd,0) as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, 
prev_total_paid_attendance_mtd/nullif(prev_capacity_mtd,0) as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average Total Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_total_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_total_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_house_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_house_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average Raw House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_raw_house_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_raw_house_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average Smackdown House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_smackdown_house_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_smackdown_house_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average CMB House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_cmb_house_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_cmb_house_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_tv_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_tv_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average Raw TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_raw_tv_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_raw_tv_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average Smackdown TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_smackdown_tv_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_smackdown_tv_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average CMB TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_cmb_tv_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_cmb_tv_event_attendance_mtd as prev_year_value
from #dp_mthly
union all
select 'MTD' as granularity, platform, 'Average PPV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_ppv_event_attendance_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_ppv_event_attendance_mtd as prev_year_value
from #dp_mthly
);

--pivot yearly dataset
drop table if exists #dp_yrly_pivot;
create table #dp_yrly_pivot as
select * from
(
select 'YTD' as granularity, platform, 'Number of Total Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_total_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_total_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_house_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_house_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of Raw House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_raw_house_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_raw_house_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of Smackdown House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_smackdown_house_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_smackdown_house_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of Combined House Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_combined_house_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_combined_house_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_tv_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_tv_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of Raw TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_raw_tv_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_raw_tv_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of Smackdown TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_smackdown_tv_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_smackdown_tv_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of Combined TV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_combined_tv_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_combined_tv_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Number of PPV Events' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, no_of_ppv_events_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_no_of_ppv_events_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Total Paid Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_total_paid_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Capacity' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, capacity_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_capacity_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Total Paid Utilization' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_utilization_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_total_paid_utilization_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Total Paid Utilization New' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, total_paid_attendance_ytd/nullif(capacity_ytd,0) as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, 
prev_total_paid_attendance_ytd/nullif(prev_capacity_ytd,0)  as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average Total Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_total_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_total_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_house_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_house_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average Raw House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_raw_house_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_raw_house_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average Smackdown House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_smackdown_house_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_smackdown_house_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average CMB House Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_cmb_house_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_cmb_house_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_tv_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_tv_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average Raw TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_raw_tv_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_raw_tv_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average Smackdown TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_smackdown_tv_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_smackdown_tv_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average CMB TV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_cmb_tv_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_cmb_tv_event_attendance_ytd as prev_year_value
from #dp_yrly
union all
select 'YTD' as granularity, platform, 'Average PPV Event Attendance' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, avg_ppv_event_attendance_ytd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_avg_ppv_event_attendance_ytd as prev_year_value
from #dp_yrly
);

drop table if exists #final_dp;
create table #final_dp as
select 
a.granularity, a.platform, a.platform as type, a.metric, a.cal_year as year,
a.cal_mth_num as month, a.cal_year_week_num_mon as week, a.cal_year_mon_week_begin_date as start_date,
a.cal_year_mon_week_end_date as end_date, a.value, a.prev_cal_year as prev_year,
a.prev_cal_year_week_num_mon as prev_year_week, a.prev_cal_year_mon_week_begin_date as prev_year_start_date,
a.prev_cal_year_mon_week_end_date as prev_year_end_date,a.prev_year_value
from 
(select * from #dp_wkly_pivot union all
 select * from #dp_mthly_pivot union all
 select * from #dp_yrly_pivot) a;"]})}}
select granularity, platform, type, metric, a.year, a.month, week, 
case when granularity = 'MTD' then b.start_date 
     when granularity = 'YTD' then c.start_date else a.start_date end as start_date,
end_date, value, prev_year, prev_year_week, 
case when granularity = 'MTD' then b.prev_year_start_date 
     when granularity = 'YTD' then c.prev_year_start_date else a.prev_year_start_date end as prev_year_start_date,     
prev_year_end_date, prev_year_value,
'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', sysdate),'YYYY_MM_DD_HH_MI_SS')+'_CP' etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id,
    convert_timezone('AMERICA/NEW_YORK', sysdate)                                   AS etl_insert_rec_dttm,
    cast (NULL as varchar)                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm	
from #final_dp a
left join
(select year,month, min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from #final_dp group by 1,2) b
on a.year = b.year
and a.month = b.month
left join
(select year,min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from #final_dp group by 1 ) c
on a.year = c.year
order by platform, granularity, metric, year, week