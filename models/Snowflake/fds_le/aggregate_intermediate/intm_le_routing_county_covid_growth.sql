{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    extract(week FROM date) AS week_num,
    extract(year FROM date) AS year_num,
    CASE
        WHEN (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg)) <= (-50 *
            new_cases_14day_moving_avg)
        THEN 'Dark Green'
        WHEN (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg)) <= (-10 *
            new_cases_14day_moving_avg)
        THEN 'Light Green'
        WHEN (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg)) < ( 10 *
            new_cases_14day_moving_avg)
        THEN 'Grey'
        WHEN (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg)) < ( 50 *
            new_cases_14day_moving_avg)
        THEN 'Light Red'
        ELSE 'Dark Red'
    END AS covid_growth_indicator
FROM
    {{ref('intm_le_routing_county_cases_deaths')}}