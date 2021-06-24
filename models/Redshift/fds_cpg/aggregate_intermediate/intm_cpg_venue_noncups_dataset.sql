

{{
  config({
		"materialized": 'ephemeral'
  })
}}


SELECT *
FROM
    {{ref('intm_cpg_venue_dataset')}}
WHERE
    (
        Event_key,Item_key) NOT IN
    (
        SELECT
            EVENT_KEY,
            ITEM_KEY
        FROM
            {{ref('intm_cpg_venue_cups_dataset')}} ) 