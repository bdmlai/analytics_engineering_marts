{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.*, b.initial_claims as initial_claims_14day_prior, b.continued_claims as continued_claims_14day_prior
from {{ref('intm_le_weekly_us_unemployment_data')}} a 
left join {{ref('intm_le_weekly_us_unemployment_data')}} b on a.state = b.state and a.rank = b.rank - 2