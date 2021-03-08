
with base as
(

    )
select * from base
union all
 select
        account_name,
        as_on_date,
      cast('as' as varchar) as country_cd,
      cast("as" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
    union all  
	    select
        account_name,
        as_on_date,
      cast('do' as varchar) as country_cd,
      cast("do" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
    union all
	    select
        account_name,
        as_on_date,
      cast('in' as varchar) as country_cd,
      cast("in" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
    union all
    select
        account_name,
        as_on_date,
      cast('is' as varchar) as country_cd,
      cast("is" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
	union all
    select
        account_name,
        as_on_date,
      cast('to' as varchar) as country_cd,
      cast("to" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"