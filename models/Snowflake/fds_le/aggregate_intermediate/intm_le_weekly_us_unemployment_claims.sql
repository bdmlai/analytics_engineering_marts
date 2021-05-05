{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    a.*,
    b.initial_claims   AS initial_claims_14day_prior,
    b.continued_claims AS continued_claims_14day_prior
FROM
    {{ref('intm_le_weekly_us_unemployment_data')}} a
LEFT JOIN
    {{ref('intm_le_weekly_us_unemployment_data')}} b
ON
    a.state = b.state
AND a.rank = b.rank - 2