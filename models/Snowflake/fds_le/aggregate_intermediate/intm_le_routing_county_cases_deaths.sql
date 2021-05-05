{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    AVG(retail_and_recreation_percent_change_from_baseline) over(partition BY state, county_name
    ORDER BY date ASC rows 13 preceding) AS
    retail_and_recreation_percent_change_from_baseline_14day_moving_avg,
    AVG(grocery_and_pharmacy_percent_change_from_baseline) over(partition BY state, county_name
    ORDER BY date ASC rows 13 preceding) AS
    grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg,
    AVG(parks_percent_change_from_baseline) over(partition BY state, county_name ORDER BY date ASC
    rows 13 preceding) AS parks_percent_change_from_baseline_14day_moving_avg,
    AVG(transit_stations_percent_change_from_baseline) over(partition BY state, county_name
    ORDER BY date ASC rows 13 preceding) AS
    transit_stations_percent_change_from_baseline_14day_moving_avg,
    AVG(workplaces_percent_change_from_baseline) over(partition BY state, county_name ORDER BY date
    ASC rows 13 preceding) AS workplaces_percent_change_from_baseline_14day_moving_avg,
    AVG(residential_percent_change_from_baseline) over(partition BY state, county_name ORDER BY
    date ASC rows 13 preceding)        AS residential_percent_change_from_baseline_14day_moving_avg,
    AVG(total_deaths) over(partition BY state,county_name ORDER BY date ASC rows 13 preceding) AS
    total_deaths_14day_moving_avg,
    AVG(total_cases) over(partition BY state,county_name ORDER BY date ASC rows 13 preceding) AS
    total_cases_14day_moving_avg,
    AVG(new_deaths) over(partition BY state,county_name ORDER BY date ASC rows 13 preceding) AS
    new_deaths_14day_moving_avg,
    AVG(new_cases) over(partition BY state,county_name ORDER BY date ASC rows 13 preceding) AS
    new_cases_14day_moving_avg,
    AVG(total_deaths) over(partition BY state,county_name ORDER BY date ASC rows 2 preceding) AS
    total_deaths_3day_moving_avg,
    AVG(total_cases) over(partition BY state,county_name ORDER BY date ASC rows 2 preceding) AS
    total_cases_3day_moving_avg,
    AVG(new_deaths) over(partition BY state,county_name ORDER BY date ASC rows 6 preceding) AS
    new_deaths_7day_moving_avg,
    AVG(new_cases) over(partition BY state,county_name ORDER BY date ASC rows 6 preceding) AS
    new_cases_7day_moving_avg,
    AVG(new_deaths) over(partition BY state,county_name ORDER BY date ASC rows 4 preceding) AS
    new_deaths_5day_moving_avg,
    AVG(new_cases) over(partition BY state,county_name ORDER BY date ASC rows 4 preceding) AS
    new_cases_5day_moving_avg,
    AVG(new_deaths) over(partition BY state,county_name ORDER BY date ASC rows 2 preceding) AS
    new_deaths_3day_moving_avg,
    AVG(new_cases) over(partition BY state,county_name ORDER BY date ASC rows 2 preceding) AS
    new_cases_3day_moving_avg
FROM
    (
        SELECT
            a.*,
            b.total_deaths                               AS prev_day_deaths,
            b.total_cases                                AS prev_day_cases,
            greatest(a.total_deaths - b.total_deaths, 0) AS new_deaths,
            greatest(a.total_cases - b.total_cases,0)    AS new_cases,
            c.total_deaths                               AS total_deaths_14day_prior,
            c.total_cases                                AS total_cases_14day_prior,
            greatest(a.total_deaths - c.total_deaths, 0) AS total_deaths_14day_prior_diff,
            greatest(a.total_cases - c.total_cases, 0)   AS total_cases_14day_prior_diff
        FROM
            {{ref('intm_le_routing_county_data')}} a
        LEFT JOIN
            {{ref('intm_le_routing_county_data')}} b
        ON
            a.state = b.state
        AND a.county_name = b.county_name
        AND a.rank1 = b.rank1 - 1
        LEFT JOIN
            {{ref('intm_le_routing_county_data')}} c
        ON
            a.state = c.state
        AND a.county_name = c.county_name
        AND a.rank1 = c.rank1 - 14)