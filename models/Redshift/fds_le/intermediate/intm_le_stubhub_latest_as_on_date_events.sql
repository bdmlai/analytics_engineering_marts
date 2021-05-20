{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    {{ref('intm_le_stubhub_all_events')}}
WHERE
    eventdatelocal_2 IN
                        (
                        SELECT DISTINCT
                            eventdatelocal_2
                        FROM
                             {{ref('intm_le_stubhub_latest_as_on_date_eventdates')}}) 