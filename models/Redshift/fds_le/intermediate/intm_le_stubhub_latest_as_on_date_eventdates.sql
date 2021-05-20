{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT DISTINCT
    eventdatelocal_2
FROM
    {{ref('intm_le_stubhub_all_events')}}
WHERE
    as_on_date_rank = 1
INTERSECT
SELECT DISTINCT
    eventdatelocal_2
FROM
    {{ref('intm_le_stubhub_all_events')}}
WHERE
    as_on_date_rank = 2
INTERSECT
SELECT DISTINCT
    eventdatelocal_2
FROM
    {{ref('intm_le_stubhub_all_events')}}
WHERE
    as_on_date_rank = 3