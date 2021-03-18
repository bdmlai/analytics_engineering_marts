{{
  config({
		"schema": 'fds_nl',
		"materialized": 'ephemeral'
  })
}}


select 		distinct broadcast_fin_week_begin_date, 
			case when src_series_name = 'WWE ENTERTAINMENT' then src_total_duration end as raw_duration,
			case when src_series_name = 'WWE NXT' then src_total_duration end as nxt_duration,
			case when src_series_name like '%smackdown%' then src_total_duration end as smd_duration,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'h2-999' then avg_audience_pct_nw_cvg_area end as coverage_hh_rating_raw,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'h2-999' then avg_audience_pct_nw_cvg_area end as coverage_hh_rating_nxt,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'h2-999' then avg_audience_pct end as national_hh_rating_raw,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'h2-999' then avg_audience_pct end as national_hh_rating_smackdown_fox,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'h2-999' then avg_audience_pct end as national_hh_rating_nxt,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'p2-999' then avg_audience_proj_000 end as raw_avg_viewers_total,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'm2-999' then avg_audience_proj_000 end as raw_avg_viewers_male,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'f2-999' then avg_audience_proj_000 end as raw_avg_viewers_female,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'p2-17' then avg_audience_proj_000 end as raw_avg_viewers_ages_2_17,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'p18-34' then avg_audience_proj_000 end as raw_avg_viewers_ages_18_34,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'p35-54' then avg_audience_proj_000 end as raw_avg_viewers_ages_35_54,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'p55-999' then avg_audience_proj_000 end as raw_avg_viewers_age_55_plus,
			case when src_series_name = 'wwe entertainment' and src_demographic_group = 'p18-49' then avg_audience_proj_000 end as raw_avg_viewers_ages_18_49,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'p2-999' then avg_audience_proj_000 end as sd_avg_viewers_total,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'm2-999' then avg_audience_proj_000 end as sd_avg_viewers_male,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'f2-999' then avg_audience_proj_000 end as sd_avg_viewers_female,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'p2-17' then avg_audience_proj_000 end as sd_avg_viewers_ages_2_17,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'p18-34' then avg_audience_proj_000 end as sd_avg_viewers_ages_18_34,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'p35-54' then avg_audience_proj_000 end as sd_avg_viewers_ages_35_54,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'p55-999' then avg_audience_proj_000 end as sd_avg_viewers_age_55_plus,
			case when src_series_name like '%smackdown%' and src_demographic_group = 'p18-49' then avg_audience_proj_000 end as sd_avg_viewers_ages_18_49,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'p2-999' then avg_audience_proj_000 end as nxt_avg_viewers_total,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'm2-999' then avg_audience_proj_000 end as nxt_avg_viewers_male,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'f2-999' then avg_audience_proj_000 end as nxt_avg_viewers_female,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'p2-17' then avg_audience_proj_000 end as nxt_avg_viewers_ages_2_17,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'p18-34' then avg_audience_proj_000 end as nxt_avg_viewers_ages_18_34,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'p35-54' then avg_audience_proj_000 end as nxt_avg_viewers_ages_35_54,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'p55-999' then avg_audience_proj_000 end as nxt_avg_viewers_age_55_plus,
			case when src_series_name = 'wwe nxt' and src_demographic_group = 'p18-49' then avg_audience_proj_000 end as nxt_avg_viewers_ages_18_49

from 		{{ ref("intm_nl_daily_wwe_program_ratings") }}

where 		src_playback_period_cd = 'Live+SD' and src_daypart_name = 'PRIME TIME' and src_program_id in (436999,296881,898521,1000131,339681)
			and src_demographic_group in ('H2-999','P2-999','M2-999','F2-999','P2-17','P18-34','P35-54','P55-999','P18-49') 