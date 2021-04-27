{{
  config({
		"schema": 'fds_nl',
		"materialized": 'ephemeral',"tags": 'rpt_tv_weekly_consolidated_kpi'
  })
}}


select distinct broadcast_fin_week_begin_date, 
case when src_series_name = 'WWE ENTERTAINMENT' then src_total_duration end as raw_duration,
case when src_series_name = 'WWE NXT' then src_total_duration end as nxt_duration,
case when src_series_name like '%SMACKDOWN%' then src_total_duration end as smd_duration,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct_nw_cvg_area END AS coverage_hh_rating_raw,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct_nw_cvg_area END AS coverage_hh_rating_nxt,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct END AS national_hh_rating_raw,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'H2-999' THEN avg_audience_pct END AS national_hh_rating_smackdown_fox,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'H2-999' THEN avg_audience_pct END AS national_hh_rating_nxt,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P2-999' THEN avg_audience_proj_000 END AS raw_avg_viewers_total,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'M2-999' THEN avg_audience_proj_000 END AS raw_avg_viewers_male,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'F2-999' THEN avg_audience_proj_000 END AS raw_avg_viewers_female,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P2-17' THEN avg_audience_proj_000 END AS raw_avg_viewers_ages_2_17,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P18-34' THEN avg_audience_proj_000 END AS raw_avg_viewers_ages_18_34,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P35-54' THEN avg_audience_proj_000 END AS raw_avg_viewers_ages_35_54,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P55-999' THEN avg_audience_proj_000 END AS raw_avg_viewers_age_55_plus,
case when src_series_name = 'WWE ENTERTAINMENT' AND src_demographic_group = 'P18-49' THEN avg_audience_proj_000 END AS raw_avg_viewers_ages_18_49,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P2-999' THEN avg_audience_proj_000 END AS sd_avg_viewers_total,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'M2-999' THEN avg_audience_proj_000 END AS sd_avg_viewers_male,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'F2-999' THEN avg_audience_proj_000 END AS sd_avg_viewers_female,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P2-17' THEN avg_audience_proj_000 END AS sd_avg_viewers_ages_2_17,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P18-34' THEN avg_audience_proj_000 END AS sd_avg_viewers_ages_18_34,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P35-54' THEN avg_audience_proj_000 END AS sd_avg_viewers_ages_35_54,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P55-999' THEN avg_audience_proj_000 END AS sd_avg_viewers_age_55_plus,
case when src_series_name like '%SMACKDOWN%' AND src_demographic_group = 'P18-49' THEN avg_audience_proj_000 END AS sd_avg_viewers_ages_18_49,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P2-999' THEN avg_audience_proj_000 END AS nxt_avg_viewers_total,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'M2-999' THEN avg_audience_proj_000 END AS nxt_avg_viewers_male,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'F2-999' THEN avg_audience_proj_000 END AS nxt_avg_viewers_female,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P2-17' THEN avg_audience_proj_000 END AS nxt_avg_viewers_ages_2_17,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P18-34' THEN avg_audience_proj_000 END AS nxt_avg_viewers_ages_18_34,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P35-54' THEN avg_audience_proj_000 END AS nxt_avg_viewers_ages_35_54,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P55-999' THEN avg_audience_proj_000 END AS nxt_avg_viewers_age_55_plus,
case when src_series_name = 'WWE NXT' AND src_demographic_group = 'P18-49' THEN avg_audience_proj_000 END AS nxt_avg_viewers_ages_18_49

from 		{{ source('fds_nl', 'rpt_nl_daily_wwe_program_ratings') }} 

where 		src_playback_period_cd = 'Live+SD' and src_daypart_name = 'PRIME TIME' and src_program_id in (436999,296881,898521,1000131,339681)
			and src_demographic_group in ('H2-999','P2-999','M2-999','F2-999','P2-17','P18-34','P35-54','P55-999','P18-49') 