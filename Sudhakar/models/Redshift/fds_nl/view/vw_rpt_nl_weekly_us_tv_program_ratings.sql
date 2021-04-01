{{
  config({
	'schema': 'fds_nl',"materialized": 'view','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true},
	'post-hook': 'grant select on {{ this }} to public'
       
	})
}}

select  week
        ,year
        ,week_number
        ,program_type
        ,src_demographic_group
        ,src_playback_period_cd
        ,avg_audience_proj_000
        , avg_audience_pct 
 from {{ref('rpt_nl_weekly_us_tv_program_ratings')}}