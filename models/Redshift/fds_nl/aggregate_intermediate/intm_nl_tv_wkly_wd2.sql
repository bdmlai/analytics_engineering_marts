{{
  config({
		"schema": 'fds_nl',
		"materialized": 'table'
  })
}}


with tv_wkly_wd1 as
(
select 		b.*, 
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
			cast('TV' as text) as platform

from 		{{ ref("intm_nl_tv_wkly_dates") }} b

left join 
(
select 		broadcast_fin_week_begin_date, 
			sum(raw_duration)/count(raw_duration) as raw_duration,
			sum(nxt_duration)/count(nxt_duration) as nxt_duration,
			sum(smd_duration)/count(smd_duration) as smd_duration,
			sum(coalesce(coverage_hh_rating_raw,0)) as coverage_hh_rating_raw,
			sum(coalesce(coverage_hh_rating_nxt,0)) as coverage_hh_rating_nxt,
			sum(coalesce(national_hh_rating_raw,0)) as national_hh_rating_raw,
			sum(coalesce(national_hh_rating_smackdown_fox,0)) as national_hh_rating_smackdown_fox,
			sum(coalesce(national_hh_rating_nxt,0)) as national_hh_rating_nxt,
			sum(coalesce(raw_avg_viewers_total,0)) as raw_avg_viewers_total,
			sum(coalesce(raw_avg_viewers_male,0)) as raw_avg_viewers_male,
			sum(coalesce(raw_avg_viewers_female,0)) as raw_avg_viewers_female,
			sum(coalesce(raw_avg_viewers_ages_2_17,0)) as raw_avg_viewers_ages_2_17,
			sum(coalesce(raw_avg_viewers_ages_18_34,0)) as raw_avg_viewers_ages_18_34,
			sum(coalesce(raw_avg_viewers_ages_35_54,0)) as raw_avg_viewers_ages_35_54,
			sum(coalesce(raw_avg_viewers_age_55_plus,0)) as raw_avg_viewers_age_55_plus,
			sum(coalesce(raw_avg_viewers_ages_18_49,0)) as raw_avg_viewers_ages_18_49,
			sum(coalesce(sd_avg_viewers_total,0)) as sd_avg_viewers_total,
			sum(coalesce(sd_avg_viewers_male,0)) as sd_avg_viewers_male,
			sum(coalesce(sd_avg_viewers_female,0)) as sd_avg_viewers_female,
			sum(coalesce(sd_avg_viewers_ages_2_17,0)) as sd_avg_viewers_ages_2_17,
			sum(coalesce(sd_avg_viewers_ages_18_34,0)) as sd_avg_viewers_ages_18_34,
			sum(coalesce(sd_avg_viewers_ages_35_54,0)) as sd_avg_viewers_ages_35_54,
			sum(coalesce(sd_avg_viewers_age_55_plus,0)) as sd_avg_viewers_age_55_plus,
			sum(coalesce(sd_avg_viewers_ages_18_49,0)) as sd_avg_viewers_ages_18_49,
			sum(coalesce(nxt_avg_viewers_total,0)) as nxt_avg_viewers_total,
			sum(coalesce(nxt_avg_viewers_male,0)) as nxt_avg_viewers_male,
			sum(coalesce(nxt_avg_viewers_female,0)) as nxt_avg_viewers_female,
			sum(coalesce(nxt_avg_viewers_ages_2_17,0)) as nxt_avg_viewers_ages_2_17,
			sum(coalesce(nxt_avg_viewers_ages_18_34,0)) as nxt_avg_viewers_ages_18_34,
			sum(coalesce(nxt_avg_viewers_ages_35_54,0)) as nxt_avg_viewers_ages_35_54,
			sum(coalesce(nxt_avg_viewers_age_55_plus,0)) as nxt_avg_viewers_age_55_plus,
			sum(coalesce(nxt_avg_viewers_ages_18_49,0)) as nxt_avg_viewers_ages_18_49

from 		{{ ref("intm_nl_tv_wkly_wd") }}

group by 	1 

order by 	1

) a 		on a.broadcast_fin_week_begin_date = b.cal_year_mon_week_begin_date
)


select 		a.*, 
			b.cal_year as prev_year, 
			b.cal_year_week_num_mon as prev_year_week,
			b.cal_year_mon_week_begin_date as prev_year_start_date, 
			b.cal_year_mon_week_end_date as prev_year_end_date,
			coalesce(b.raw_duration,0) as prev_raw_duration,
			coalesce(b.nxt_duration,0) as prev_nxt_duration,
			coalesce(b.smd_duration,0) as prev_smd_duration,
			coalesce(b.coverage_hh_rating_raw,0) as prev_coverage_hh_rating_raw,
			coalesce(b.coverage_hh_rating_nxt,0) as prev_coverage_hh_rating_nxt,
			coalesce(b.national_hh_rating_raw,0) as prev_national_hh_rating_raw,
			coalesce(b.national_hh_rating_smackdown_fox,0) as prev_national_hh_rating_smackdown_fox,
			coalesce(b.national_hh_rating_nxt,0) as prev_national_hh_rating_nxt,
			coalesce(b.raw_avg_viewers_total,0) as prev_raw_avg_viewers_total,
			coalesce(b.raw_avg_viewers_male,0) as prev_raw_avg_viewers_male,
			coalesce(b.raw_avg_viewers_female,0) as prev_raw_avg_viewers_female,
			coalesce(b.raw_avg_viewers_ages_2_17,0) as prev_raw_avg_viewers_ages_2_17,
			coalesce(b.raw_avg_viewers_ages_18_34,0) as prev_raw_avg_viewers_ages_18_34,
			coalesce(b.raw_avg_viewers_ages_35_54,0) as prev_raw_avg_viewers_ages_35_54,
			coalesce(b.raw_avg_viewers_age_55_plus,0) as prev_raw_avg_viewers_age_55_plus,
			coalesce(b.raw_avg_viewers_ages_18_49,0) as prev_raw_avg_viewers_ages_18_49,
			coalesce(b.sd_avg_viewers_total,0) as prev_sd_avg_viewers_total,
			coalesce(b.sd_avg_viewers_male,0) as prev_sd_avg_viewers_male,
			coalesce(b.sd_avg_viewers_female,0) as prev_sd_avg_viewers_female,
			coalesce(b.sd_avg_viewers_ages_2_17,0) as prev_sd_avg_viewers_ages_2_17,
			coalesce(b.sd_avg_viewers_ages_18_34,0) as prev_sd_avg_viewers_ages_18_34,
			coalesce(b.sd_avg_viewers_ages_35_54,0) as prev_sd_avg_viewers_ages_35_54,
			coalesce(b.sd_avg_viewers_age_55_plus,0) as prev_sd_avg_viewers_age_55_plus,
			coalesce(b.sd_avg_viewers_ages_18_49,0) as prev_sd_avg_viewers_ages_18_49,
			coalesce(b.nxt_avg_viewers_total,0) as prev_nxt_avg_viewers_total,
			coalesce(b.nxt_avg_viewers_male,0) as prev_nxt_avg_viewers_male,
			coalesce(b.nxt_avg_viewers_female,0) as prev_nxt_avg_viewers_female,
			coalesce(b.nxt_avg_viewers_ages_2_17,0) as prev_nxt_avg_viewers_ages_2_17,
			coalesce(b.nxt_avg_viewers_ages_18_34,0) as prev_nxt_avg_viewers_ages_18_34,
			coalesce(b.nxt_avg_viewers_ages_35_54,0) as prev_nxt_avg_viewers_ages_35_54,
			coalesce(b.nxt_avg_viewers_age_55_plus,0) as prev_nxt_avg_viewers_age_55_plus,
			coalesce(b.nxt_avg_viewers_ages_18_49,0) as prev_nxt_avg_viewers_ages_18_49

from 		tv_wkly_wd1 a

left join 	tv_wkly_wd1 b on (a.cal_year-1) = b.cal_year and a.cal_year_week_num_mon = b.cal_year_week_num_mon and a.platform = b.platform

