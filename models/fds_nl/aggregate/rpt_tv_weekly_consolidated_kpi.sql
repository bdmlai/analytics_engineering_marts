 {{
  config({
	"schema": 'fds_nl',	
	"materialized": 'incremental',
	"pre-hook": ["delete from fds_nl.rpt_tv_weekly_consolidated_kpi",
"--create dates for rollup
drop table if exists #dim_dates;
create table #dim_dates as
select distinct cal_year, --extract('month' from cal_year_mon_week_begin_date) as cal_mth_num, 
case when cal_year = extract('year' from cal_year_mon_week_begin_date) then extract('month' from cal_year_mon_week_begin_date)
     when cal_year = extract('year' from cal_year_mon_week_end_date) then extract('month' from cal_year_mon_week_end_date)   
     end as cal_mth_num, 
case when cal_year_week_num_mon is null then 1 else cal_year_week_num_mon end as cal_year_week_num_mon,
cal_year_mon_week_begin_date, cal_year_mon_week_end_date
from cdm.dim_date where cal_year_mon_week_begin_date >= trunc(dateadd('year',-2,date_trunc('year',getdate()))) 
and cal_year_mon_week_end_date < date_trunc('week',getdate());

--create TV weekly dataset
drop table if exists #tv_wkly_wd;
create table #tv_wkly_wd as
select distinct broadcast_fin_week_begin_date, 
case when src_series_name = 'WWE ENTERTAINMENT' then src_total_duration end as raw_duration,
case when src_series_name = 'WWE NXT' then src_total_duration end as nxt_duration,
case when src_series_name like '%SMACKDOWN%' then src_total_duration end as smd_duration,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct_nw_cvg_area END AS Coverage_HH_Rating_Raw,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct_nw_cvg_area END AS Coverage_HH_Rating_NXT,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct END AS National_HH_Rating_Raw,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'H2-999' THEN avg_audience_pct END AS National_HH_Rating_Smackdown_Fox,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct END AS National_HH_Rating_NXT,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P2-999' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Total,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'M2-999' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Male,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'F2-999' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Female,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P2-17' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Ages_2_17,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P18-34' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Ages_18_34,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P35-54' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Ages_35_54,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P55-999' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Age_55_plus,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P18-49' THEN avg_audience_proj_000 END AS Raw_Avg_Viewers_Ages_18_49,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P2-999' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Total,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'M2-999' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Male,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'F2-999' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Female,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P2-17' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Ages_2_17,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P18-34' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Ages_18_34,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P35-54' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Ages_35_54,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P55-999' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Age_55_plus,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P18-49' THEN avg_audience_proj_000 END AS SD_Avg_Viewers_Ages_18_49,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P2-999' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Total,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'M2-999' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Male,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'F2-999' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Female,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P2-17' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Ages_2_17,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P18-34' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Ages_18_34,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P35-54' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Ages_35_54,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P55-999' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Age_55_plus,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P18-49' THEN avg_audience_proj_000 END AS NXT_Avg_Viewers_Ages_18_49
from fds_nl.rpt_nl_daily_wwe_program_ratings
where src_playback_period_cd = 'Live+SD'
and src_daypart_name = 'PRIME TIME'
--and src_series_name in ('WWE FRI NIGHT SMACKDOWN', 'WWE ENTERTAINMENT', 'WWE NXT','WWE SMACKDOWN')
and src_demographic_group in ('H2-999','P2-999','M2-999','F2-999','P2-17','P18-34','P35-54','P55-999','P18-49')
and src_program_id in (436999,296881,898521,1000131,339681)
--and broadcast_network_name in ('USA NETWORK','FOX')
;
drop table if exists #tv_wkly_wd1;
create table #tv_wkly_wd1 as
select b.*, 
a.raw_duration,
a.nxt_duration,
a.smd_duration,
a.coverage_hh_rating_raw,
a.coverage_hh_rating_nxt, 
a.national_hh_rating_raw,
a.national_hh_rating_smackdown_fox,
a.national_hh_rating_nxt,
a.raw_avg_viewers_total,
a.raw_avg_viewers_male,
a.raw_avg_viewers_female,
a.raw_avg_viewers_ages_2_17,
a.raw_avg_viewers_ages_18_34,
a.raw_avg_viewers_ages_35_54,
a.raw_avg_viewers_age_55_plus,
a.raw_avg_viewers_ages_18_49,
a.sd_avg_viewers_total,
a.sd_avg_viewers_male,
a.sd_avg_viewers_female,
a.sd_avg_viewers_ages_2_17,
a.sd_avg_viewers_ages_18_34,
a.sd_avg_viewers_ages_35_54,
a.sd_avg_viewers_age_55_plus,
a.sd_avg_viewers_ages_18_49,
a.nxt_avg_viewers_total,
a.nxt_avg_viewers_male,
a.nxt_avg_viewers_female,
a.nxt_avg_viewers_ages_2_17,
a.nxt_avg_viewers_ages_18_34,
a.nxt_avg_viewers_ages_35_54,
a.nxt_avg_viewers_age_55_plus,
a.nxt_avg_viewers_ages_18_49,
'TV' as platform
from 
#dim_dates b
left join 
(
select broadcast_fin_week_begin_date, 
sum(raw_duration)/count(raw_duration) as raw_duration,
sum(nxt_duration)/count(nxt_duration) as nxt_duration,
sum(smd_duration)/count(smd_duration) as smd_duration,
sum(coalesce(coverage_hh_rating_raw,0)) coverage_hh_rating_raw,
sum(coalesce(coverage_hh_rating_nxt,0)) coverage_hh_rating_nxt,
sum(coalesce(national_hh_rating_raw,0)) national_hh_rating_raw,
sum(coalesce(national_hh_rating_smackdown_fox,0)) national_hh_rating_smackdown_fox,
sum(coalesce(national_hh_rating_nxt,0)) national_hh_rating_nxt,
sum(coalesce(raw_avg_viewers_total,0)) raw_avg_viewers_total,
sum(coalesce(raw_avg_viewers_male,0)) raw_avg_viewers_male,
sum(coalesce(raw_avg_viewers_female,0)) raw_avg_viewers_female,
sum(coalesce(raw_avg_viewers_ages_2_17,0)) raw_avg_viewers_ages_2_17,
sum(coalesce(raw_avg_viewers_ages_18_34,0)) raw_avg_viewers_ages_18_34,
sum(coalesce(raw_avg_viewers_ages_35_54,0)) raw_avg_viewers_ages_35_54,
sum(coalesce(raw_avg_viewers_age_55_plus,0)) raw_avg_viewers_age_55_plus,
sum(coalesce(raw_avg_viewers_ages_18_49,0)) raw_avg_viewers_ages_18_49,
sum(coalesce(sd_avg_viewers_total,0)) sd_avg_viewers_total,
sum(coalesce(sd_avg_viewers_male,0)) sd_avg_viewers_male,
sum(coalesce(sd_avg_viewers_female,0)) sd_avg_viewers_female,
sum(coalesce(sd_avg_viewers_ages_2_17,0)) sd_avg_viewers_ages_2_17,
sum(coalesce(sd_avg_viewers_ages_18_34,0)) sd_avg_viewers_ages_18_34,
sum(coalesce(sd_avg_viewers_ages_35_54,0)) sd_avg_viewers_ages_35_54,
sum(coalesce(sd_avg_viewers_age_55_plus,0)) sd_avg_viewers_age_55_plus,
sum(coalesce(sd_avg_viewers_ages_18_49,0)) sd_avg_viewers_ages_18_49,
sum(coalesce(nxt_avg_viewers_total,0)) nxt_avg_viewers_total,
sum(coalesce(nxt_avg_viewers_male,0)) nxt_avg_viewers_male,
sum(coalesce(nxt_avg_viewers_female,0)) nxt_avg_viewers_female,
sum(coalesce(nxt_avg_viewers_ages_2_17,0)) nxt_avg_viewers_ages_2_17,
sum(coalesce(nxt_avg_viewers_ages_18_34,0)) nxt_avg_viewers_ages_18_34,
sum(coalesce(nxt_avg_viewers_ages_35_54,0)) nxt_avg_viewers_ages_35_54,
sum(coalesce(nxt_avg_viewers_age_55_plus,0)) nxt_avg_viewers_age_55_plus,
sum(coalesce(nxt_avg_viewers_ages_18_49,0)) nxt_avg_viewers_ages_18_49
from #tv_wkly_wd
group by 1 order by 1) a
on a.broadcast_fin_week_begin_date = b.cal_year_mon_week_begin_date;

drop table if exists #tv_wkly_wd2;
create table #tv_wkly_wd2 as
select a.*, 
b.cal_year as prev_year, b.cal_year_week_num_mon as prev_year_week,
b.cal_year_mon_week_begin_date as prev_year_start_date, b.cal_year_mon_week_end_date as prev_year_end_date,
coalesce(b.raw_duration,0) prev_raw_duration,
coalesce(b.nxt_duration,0) prev_nxt_duration,
coalesce(b.smd_duration,0) prev_smd_duration,
coalesce(b.coverage_hh_rating_raw,0) prev_coverage_hh_rating_raw,
coalesce(b.coverage_hh_rating_nxt,0) prev_coverage_hh_rating_nxt,
coalesce(b.national_hh_rating_raw,0) prev_national_hh_rating_raw,
coalesce(b.national_hh_rating_smackdown_fox,0) prev_national_hh_rating_smackdown_fox,
coalesce(b.national_hh_rating_nxt,0) prev_national_hh_rating_nxt,
coalesce(b.raw_avg_viewers_total,0) prev_raw_avg_viewers_total,
coalesce(b.raw_avg_viewers_male,0) prev_raw_avg_viewers_male,
coalesce(b.raw_avg_viewers_female,0) prev_raw_avg_viewers_female,
coalesce(b.raw_avg_viewers_ages_2_17,0) prev_raw_avg_viewers_ages_2_17,
coalesce(b.raw_avg_viewers_ages_18_34,0) prev_raw_avg_viewers_ages_18_34,
coalesce(b.raw_avg_viewers_ages_35_54,0) prev_raw_avg_viewers_ages_35_54,
coalesce(b.raw_avg_viewers_age_55_plus,0) prev_raw_avg_viewers_age_55_plus,
coalesce(b.raw_avg_viewers_ages_18_49,0) prev_raw_avg_viewers_ages_18_49,
coalesce(b.sd_avg_viewers_total,0) prev_sd_avg_viewers_total,
coalesce(b.sd_avg_viewers_male,0) prev_sd_avg_viewers_male,
coalesce(b.sd_avg_viewers_female,0) prev_sd_avg_viewers_female,
coalesce(b.sd_avg_viewers_ages_2_17,0) prev_sd_avg_viewers_ages_2_17,
coalesce(b.sd_avg_viewers_ages_18_34,0) prev_sd_avg_viewers_ages_18_34,
coalesce(b.sd_avg_viewers_ages_35_54,0) prev_sd_avg_viewers_ages_35_54,
coalesce(b.sd_avg_viewers_age_55_plus,0) prev_sd_avg_viewers_age_55_plus,
coalesce(b.sd_avg_viewers_ages_18_49,0) prev_sd_avg_viewers_ages_18_49,
coalesce(b.nxt_avg_viewers_total,0) prev_nxt_avg_viewers_total,
coalesce(b.nxt_avg_viewers_male,0) prev_nxt_avg_viewers_male,
coalesce(b.nxt_avg_viewers_female,0) prev_nxt_avg_viewers_female,
coalesce(b.nxt_avg_viewers_ages_2_17,0) prev_nxt_avg_viewers_ages_2_17,
coalesce(b.nxt_avg_viewers_ages_18_34,0) prev_nxt_avg_viewers_ages_18_34,
coalesce(b.nxt_avg_viewers_ages_35_54,0) prev_nxt_avg_viewers_ages_35_54,
coalesce(b.nxt_avg_viewers_age_55_plus,0) prev_nxt_avg_viewers_age_55_plus,
coalesce(b.nxt_avg_viewers_ages_18_49,0) prev_nxt_avg_viewers_ages_18_49
from #tv_wkly_wd1 a
left join
#tv_wkly_wd1 b
on (a.cal_year-1) = b.cal_year and a.cal_year_week_num_mon = b.cal_year_week_num_mon and a.platform = b.platform;

-- create TV Monthly dataset
drop table if exists #tv_mthly;
create table #tv_mthly as
select a.platform, a.cal_year as year,a.cal_mth_num as month, a.cal_year_week_num_mon as week, 
a.cal_year_mon_week_begin_date as start_date, a.cal_year_mon_week_end_date as end_date, 
a.prev_year, a.prev_year_week, a.prev_year_start_date, a.prev_year_end_date, 
sum(b.Coverage_HH_Rating_Raw*b.raw_duration)/sum(b.raw_duration) as Coverage_HH_Rating_Raw,
sum(b.Coverage_HH_Rating_NXT*b.nxt_duration)/sum(b.nxt_duration) as Coverage_HH_Rating_NXT,
sum(b.National_HH_Rating_Raw*b.raw_duration)/sum(b.raw_duration) as National_HH_Rating_Raw,
sum(b.National_HH_Rating_Smackdown_Fox*b.smd_duration)/sum(b.smd_duration) as National_HH_Rating_Smackdown_Fox,
sum(b.National_HH_Rating_NXT*b.nxt_duration)/sum(b.nxt_duration) as National_HH_Rating_NXT,
sum(b.Raw_Avg_Viewers_Total*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Total,
sum(b.Raw_Avg_Viewers_Male*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Male,
sum(b.Raw_Avg_Viewers_Female*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Female,
sum(b.Raw_Avg_Viewers_Ages_2_17*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_2_17,
sum(b.Raw_Avg_Viewers_Ages_18_34*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_18_34,
sum(b.Raw_Avg_Viewers_Ages_35_54*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_35_54,
sum(b.Raw_Avg_Viewers_Age_55_plus*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Age_55_plus,
sum(b.Raw_Avg_Viewers_Ages_18_49*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_18_49,
sum(b.SD_Avg_Viewers_Total*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Total,
sum(b.SD_Avg_Viewers_Male*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Male,
sum(b.SD_Avg_Viewers_Female*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Female,
sum(b.SD_Avg_Viewers_Ages_2_17*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_2_17,
sum(b.SD_Avg_Viewers_Ages_18_34*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_18_34,
sum(b.SD_Avg_Viewers_Ages_35_54*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_35_54,
sum(b.SD_Avg_Viewers_Age_55_plus*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Age_55_plus,
sum(b.SD_Avg_Viewers_Ages_18_49*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_18_49,
sum(b.NXT_Avg_Viewers_Total*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Total,
sum(b.NXT_Avg_Viewers_Male*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Male,
sum(b.NXT_Avg_Viewers_Female*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Female,
sum(b.NXT_Avg_Viewers_Ages_2_17*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_2_17,
sum(b.NXT_Avg_Viewers_Ages_18_34*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_18_34,
sum(b.NXT_Avg_Viewers_Ages_35_54*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_35_54,
sum(b.NXT_Avg_Viewers_Age_55_plus*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Age_55_plus,
sum(b.NXT_Avg_Viewers_Ages_18_49*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_18_49,
sum(b.prev_Coverage_HH_Rating_Raw*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Coverage_HH_Rating_Raw,
sum(b.prev_Coverage_HH_Rating_NXT*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_Coverage_HH_Rating_NXT,
sum(b.prev_National_HH_Rating_Raw*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_National_HH_Rating_Raw,
sum(b.prev_National_HH_Rating_Smackdown_Fox*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_National_HH_Rating_Smackdown_Fox,
sum(b.prev_National_HH_Rating_NXT*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_National_HH_Rating_NXT,
sum(b.prev_Raw_Avg_Viewers_Total*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Total,
sum(b.prev_Raw_Avg_Viewers_Male*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Male,
sum(b.prev_Raw_Avg_Viewers_Female*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Female,
sum(b.prev_Raw_Avg_Viewers_Ages_2_17*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_2_17,
sum(b.prev_Raw_Avg_Viewers_Ages_18_34*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_18_34,
sum(b.prev_Raw_Avg_Viewers_Ages_35_54*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_35_54,
sum(b.prev_Raw_Avg_Viewers_Age_55_plus*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Age_55_plus,
sum(b.prev_Raw_Avg_Viewers_Ages_18_49*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_18_49,
sum(b.prev_SD_Avg_Viewers_Total*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Total,
sum(b.prev_SD_Avg_Viewers_Male*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Male,
sum(b.prev_SD_Avg_Viewers_Female*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Female,
sum(b.prev_SD_Avg_Viewers_Ages_2_17*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_2_17,
sum(b.prev_SD_Avg_Viewers_Ages_18_34*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_18_34,
sum(b.prev_SD_Avg_Viewers_Ages_35_54*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_35_54,
sum(b.prev_SD_Avg_Viewers_Age_55_plus*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Age_55_plus,
sum(b.prev_SD_Avg_Viewers_Ages_18_49*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_18_49,
sum(b.prev_NXT_Avg_Viewers_Total*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Total,
sum(b.prev_NXT_Avg_Viewers_Male*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Male,
sum(b.prev_NXT_Avg_Viewers_Female*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Female,
sum(b.prev_NXT_Avg_Viewers_Ages_2_17*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_2_17,
sum(b.prev_NXT_Avg_Viewers_Ages_18_34*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_18_34,
sum(b.prev_NXT_Avg_Viewers_Ages_35_54*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_35_54,
sum(b.prev_NXT_Avg_Viewers_Age_55_plus*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Age_55_plus,
sum(b.prev_NXT_Avg_Viewers_Ages_18_49*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_18_49
from #tv_wkly_wd2 a
left join #tv_wkly_wd2 b
on a.cal_year = b.cal_year and a.cal_mth_num = b.cal_mth_num and a.cal_year_week_num_mon >= b.cal_year_week_num_mon 
group by 1,2,3,4,5,6,7,8,9,10;

--create TV yearly dataset
drop table if exists #tv_yrly;
create table #tv_yrly as
select a.platform, a.cal_year as year,a.cal_mth_num as month, a.cal_year_week_num_mon as week, 
a.cal_year_mon_week_begin_date as start_date, a.cal_year_mon_week_end_date as end_date, 
a.prev_year, a.prev_year_week, a.prev_year_start_date, a.prev_year_end_date,
sum(b.Coverage_HH_Rating_Raw*b.raw_duration)/sum(b.raw_duration) as Coverage_HH_Rating_Raw,
sum(b.Coverage_HH_Rating_NXT*b.nxt_duration)/sum(b.nxt_duration) as Coverage_HH_Rating_NXT,
sum(b.National_HH_Rating_Raw*b.raw_duration)/sum(b.raw_duration) as National_HH_Rating_Raw,
sum(b.National_HH_Rating_Smackdown_Fox*b.smd_duration)/sum(b.smd_duration) as National_HH_Rating_Smackdown_Fox,
sum(b.National_HH_Rating_NXT*b.nxt_duration)/sum(b.nxt_duration) as National_HH_Rating_NXT,
sum(b.Raw_Avg_Viewers_Total*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Total,
sum(b.Raw_Avg_Viewers_Male*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Male,
sum(b.Raw_Avg_Viewers_Female*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Female,
sum(b.Raw_Avg_Viewers_Ages_2_17*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_2_17,
sum(b.Raw_Avg_Viewers_Ages_18_34*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_18_34,
sum(b.Raw_Avg_Viewers_Ages_35_54*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_35_54,
sum(b.Raw_Avg_Viewers_Age_55_plus*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Age_55_plus,
sum(b.Raw_Avg_Viewers_Ages_18_49*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_18_49,
sum(b.SD_Avg_Viewers_Total*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Total,
sum(b.SD_Avg_Viewers_Male*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Male,
sum(b.SD_Avg_Viewers_Female*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Female,
sum(b.SD_Avg_Viewers_Ages_2_17*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_2_17,
sum(b.SD_Avg_Viewers_Ages_18_34*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_18_34,
sum(b.SD_Avg_Viewers_Ages_35_54*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_35_54,
sum(b.SD_Avg_Viewers_Age_55_plus*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Age_55_plus,
sum(b.SD_Avg_Viewers_Ages_18_49*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_18_49,
sum(b.NXT_Avg_Viewers_Total*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Total,
sum(b.NXT_Avg_Viewers_Male*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Male,
sum(b.NXT_Avg_Viewers_Female*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Female,
sum(b.NXT_Avg_Viewers_Ages_2_17*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_2_17,
sum(b.NXT_Avg_Viewers_Ages_18_34*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_18_34,
sum(b.NXT_Avg_Viewers_Ages_35_54*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_35_54,
sum(b.NXT_Avg_Viewers_Age_55_plus*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Age_55_plus,
sum(b.NXT_Avg_Viewers_Ages_18_49*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_18_49,
sum(b.prev_Coverage_HH_Rating_Raw*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Coverage_HH_Rating_Raw,
sum(b.prev_Coverage_HH_Rating_NXT*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_Coverage_HH_Rating_NXT,
sum(b.prev_National_HH_Rating_Raw*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_National_HH_Rating_Raw,
sum(b.prev_National_HH_Rating_Smackdown_Fox*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_National_HH_Rating_Smackdown_Fox,
sum(b.prev_National_HH_Rating_NXT*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_National_HH_Rating_NXT,
sum(b.prev_Raw_Avg_Viewers_Total*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Total,
sum(b.prev_Raw_Avg_Viewers_Male*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Male,
sum(b.prev_Raw_Avg_Viewers_Female*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Female,
sum(b.prev_Raw_Avg_Viewers_Ages_2_17*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_2_17,
sum(b.prev_Raw_Avg_Viewers_Ages_18_34*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_18_34,
sum(b.prev_Raw_Avg_Viewers_Ages_35_54*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_35_54,
sum(b.prev_Raw_Avg_Viewers_Age_55_plus*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Age_55_plus,
sum(b.prev_Raw_Avg_Viewers_Ages_18_49*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_18_49,
sum(b.prev_SD_Avg_Viewers_Total*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Total,
sum(b.prev_SD_Avg_Viewers_Male*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Male,
sum(b.prev_SD_Avg_Viewers_Female*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Female,
sum(b.prev_SD_Avg_Viewers_Ages_2_17*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_2_17,
sum(b.prev_SD_Avg_Viewers_Ages_18_34*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_18_34,
sum(b.prev_SD_Avg_Viewers_Ages_35_54*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_35_54,
sum(b.prev_SD_Avg_Viewers_Age_55_plus*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Age_55_plus,
sum(b.prev_SD_Avg_Viewers_Ages_18_49*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_18_49,
sum(b.prev_NXT_Avg_Viewers_Total*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Total,
sum(b.prev_NXT_Avg_Viewers_Male*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Male,
sum(b.prev_NXT_Avg_Viewers_Female*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Female,
sum(b.prev_NXT_Avg_Viewers_Ages_2_17*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_2_17,
sum(b.prev_NXT_Avg_Viewers_Ages_18_34*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_18_34,
sum(b.prev_NXT_Avg_Viewers_Ages_35_54*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_35_54,
sum(b.prev_NXT_Avg_Viewers_Age_55_plus*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Age_55_plus,
sum(b.prev_NXT_Avg_Viewers_Ages_18_49*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_18_49
from #tv_wkly_wd2 a
left join #tv_wkly_wd2 b
on a.cal_year = b.cal_year and a.cal_year_week_num_mon >= b.cal_year_week_num_mon 
group by 1,2,3,4,5,6,7,8,9,10;

--pivot weekly dataset
drop table if exists #tv_wkly_pivot;
create table #tv_wkly_pivot as
select * from
(
select 'Weekly' as granularity, platform, 'RAW' as type, 'Coverage HH rating' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Coverage_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_Raw as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'NXT' as type, 'Coverage HH rating' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Coverage_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_NXT as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'RAW' as type, 'National HH Rating ' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, National_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Raw as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'SMD' as type, 'National HH Rating ' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, National_HH_Rating_Smackdown_Fox as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Smackdown_Fox as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'NXT' as type, 'National HH Rating ' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, National_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_NXT as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Total' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Total as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Male' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Male as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Female' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Female as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 2-17' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 18-34' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 35-54' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 55+' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Age_55_plus as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 18-49' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Total' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Total as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Male' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Male as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Female' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Female as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 2-17' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 18-34' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 35-54' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 55+' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Age_55_plus as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 18-49' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Total' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Total as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Male' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Male as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Female' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Female as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 2-17' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 18-34' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 35-54' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 55+' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Age_55_plus as prev_year_value from #tv_wkly_wd2 union all
select 'Weekly' as granularity, platform, 'Ages 18-49' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_wkly_wd2
);

--pivot monthly dataset
drop table if exists #tv_mthly_pivot;
create table #tv_mthly_pivot as
select * from
(
select 'MTD' as granularity, platform, 'RAW' as type, 'Coverage HH rating' as Metric, year, month, week, start_date, end_date, Coverage_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_Raw as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'NXT' as type, 'Coverage HH rating' as Metric, year, month, week, start_date, end_date, Coverage_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_NXT as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'RAW' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Raw as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'SMD' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_Smackdown_Fox as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Smackdown_Fox as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'NXT' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_NXT as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Total' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Total as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Male' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Male as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Female' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Female as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 2-17' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 18-34' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 35-54' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 55+' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Age_55_plus as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 18-49' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Total' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Total as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Male' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Male as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Female' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Female as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 2-17' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 18-34' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 35-54' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 55+' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Age_55_plus as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 18-49' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Total' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Total as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Male' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Male as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Female' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Female as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 2-17' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 18-34' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 35-54' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 55+' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Age_55_plus as prev_year_value from #tv_mthly union all
select 'MTD' as granularity, platform, 'Ages 18-49' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_mthly
);

--pivot yearly dataset
drop table if exists #tv_yrly_pivot;
create table #tv_yrly_pivot as
select * from
(
select 'YTD' as granularity, platform, 'RAW' as type, 'Coverage HH rating' as Metric, year, month, week, start_date, end_date, Coverage_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_Raw as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'NXT' as type, 'Coverage HH rating' as Metric, year, month, week, start_date, end_date, Coverage_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_NXT as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'RAW' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Raw as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'SMD' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_Smackdown_Fox as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Smackdown_Fox as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'NXT' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_NXT as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Total' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Total as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Male' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Male as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Female' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Female as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 2-17' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 18-34' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 35-54' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 55+' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Age_55_plus as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 18-49' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Total' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Total as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Male' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Male as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Female' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Female as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 2-17' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 18-34' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 35-54' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 55+' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Age_55_plus as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 18-49' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Total' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Total as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Male' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Male as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Female' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Female as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 2-17' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_2_17 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 18-34' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_34 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 35-54' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_35_54 as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 55+' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Age_55_plus as prev_year_value from #tv_yrly union all
select 'YTD' as granularity, platform, 'Ages 18-49' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_49 as prev_year_value from #tv_yrly
);

drop table if exists #final_tv;
create table #final_tv as
select 
a.granularity, a.platform, a.type, a.metric, a.year, a.month, a.week, a.start_date, a.end_date, a.value,
a.prev_year, a.prev_year_week, a.prev_year_start_date, a.prev_year_end_date, a.prev_year_value
from 
(select * from #tv_wkly_pivot union all
select * from #tv_mthly_pivot union all
select * from #tv_yrly_pivot) a;

drop table if exists #final;
create table #final as
select granularity, platform, type, metric, a.year, a.month, week, 
case when granularity = 'MTD' then b.start_date 
     when granularity = 'YTD' then c.start_date else a.start_date end as start_date,
end_date, value, prev_year, prev_year_week, 
case when granularity = 'MTD' then b.prev_year_start_date 
     when granularity = 'YTD' then c.prev_year_start_date else a.prev_year_start_date end as prev_year_start_date,     
prev_year_end_date, prev_year_value
from #final_tv a
left join
(select year,month, min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from #final_tv group by 1,2) b
on a.year = b.year
and a.month = b.month
left join
(select year,min(start_date) start_date, min(prev_year_start_date) prev_year_start_date from #final_tv group by 1 ) c
on a.year = c.year
order by platform, granularity, metric, year, week;"]})}}
select *,'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', sysdate),'YYYY_MM_DD_HH_MI_SS')+'_CP' etl_batch_id, 
'bi_dbt_user_prd' AS etl_insert_user_id,
convert_timezone('AMERICA/NEW_YORK', sysdate) AS etl_insert_rec_dttm,
cast (NULL as varchar) AS etl_update_user_id,
CAST( NULL AS TIMESTAMP) AS etl_update_rec_dttm from #final order by platform, granularity, metric, year, week

