{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    dense_rank() over (ORDER BY (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg) /
    NULLIF(new_cases_14day_moving_avg, 0)) DESC nulls last) AS state_covid_growth_rank
FROM
    (
        SELECT
            state,
            SUM(total_cases)                AS total_cases,
            SUM(new_cases_3day_moving_avg)  AS new_cases_3day_moving_avg,
            SUM(new_cases_14day_moving_avg) AS new_cases_14day_moving_avg
        FROM
            {{ref('intm_le_routing_county_covid_growth')}}
        WHERE
            rank1 = 1
        GROUP BY
            state)