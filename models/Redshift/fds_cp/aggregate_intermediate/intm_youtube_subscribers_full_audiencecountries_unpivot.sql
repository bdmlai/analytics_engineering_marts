{{
    config({
	"materialized": 'ephemeral'
 })
}}
with base as
({{ dbt_utils.unpivot(
  relation=ref('intm_youtube_subscribers_full_audiencecountries'),
  exclude=['account_name','as_on_date'],
  remove=['accountid',"as","do","in","is","to"],
  field_name= 'country_cd',
  value_name= 'subscriber_count'
) }})
select * from base
union all
 select
        account_name,
        as_on_date,
      cast('as' as varchar) as country_cd,
      cast("as" as varchar) as subscriber_count
    from {{ref('intm_youtube_subscribers_full_audiencecountries')}}
    union all  
	    select
        account_name,
        as_on_date,
      cast('do' as varchar) as country_cd,
      cast("do" as varchar) as subscriber_count
    from {{ref('intm_youtube_subscribers_full_audiencecountries')}}
    union all
	    select
        account_name,
        as_on_date,
      cast('in' as varchar) as country_cd,
      cast("in" as varchar) as subscriber_count
    from {{ref('intm_youtube_subscribers_full_audiencecountries')}}
    union all
    select
        account_name,
        as_on_date,
      cast('is' as varchar) as country_cd,
      cast("is" as varchar) as subscriber_count
    from {{ref('intm_youtube_subscribers_full_audiencecountries')}}
	union all
    select
        account_name,
        as_on_date,
      cast('to' as varchar) as country_cd,
      cast("to" as varchar) as subscriber_count
    from {{ref('intm_youtube_subscribers_full_audiencecountries')}}