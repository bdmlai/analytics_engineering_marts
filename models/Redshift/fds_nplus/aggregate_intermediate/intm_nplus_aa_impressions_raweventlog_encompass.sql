{{
  config({
		"materialized": 'ephemeral'
  })
}}

     select start_timestamp,
start_date,
start_time,
content_id,
content_description,
duration,
end_time,
Ad_Abbreviation,
ad_category,
 ad_type
 from (
            SELECT * FROM {{ref('intm_nplus_aa_impressions_raweventlog')}}
            UNION 
            SELECT * FROM  {{ref('intm_nplus_aa_impressions_encompass')}}
) where start_date >='2020-06-01'