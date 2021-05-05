{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    a.*,
    b.initial_claims,
    b.continued_claims,
    b.covered_employment,
    b.insured_unemployment_rate,
    b.initial_claims_14day_prior,
    b.continued_claims_14day_prior
FROM
    (
        SELECT
            state,
            date,
            status_of_reopening,
            stay_at_home_order,
            mandatory_quarantine_for_travelers,
            non_essential_business_closures,
            large_gatherings_ban,
            restaurant_limits,
            bar_closures,
            face_covering_requirement,
            face_covering_requirement_bucket_group,
            large_gatherings_ban_bucket_group,
            SUM(population)                                                AS population,
            SUM(total_deaths)                                       AS state_total_deaths ,
            SUM(total_cases)                                          AS state_total_cases,
            SUM(new_deaths)                                            AS state_new_deaths,
            SUM(new_cases)                                              AS state_new_cases,
            COUNT(DISTINCT county_name)                              AS no_of_county_data_available,
            AVG(retail_and_recreation_percent_change_from_baseline_14day_moving_avg) AS
            avg_retail_and_recreation_percent_change_from_baseline_14day_moving_avg,
            AVG(grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg) AS
            avg_grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg,
            AVG(parks_percent_change_from_baseline_14day_moving_avg) AS
            avg_parks_percent_change_from_baseline_14day_moving_avg,
            AVG(transit_stations_percent_change_from_baseline_14day_moving_avg) AS
            avg_transit_stations_percent_change_from_baseline_14day_moving_avg,
            AVG(workplaces_percent_change_from_baseline_14day_moving_avg) AS
            avg_workplaces_percent_change_from_baseline_14day_moving_avg,
            AVG(residential_percent_change_from_baseline_14day_moving_avg) AS
                                      avg_residential_percent_change_from_baseline_14day_moving_avg,
            SUM(new_cases_3day_moving_avg)    AS new_cases_3day_moving_avg,
            SUM(new_cases_14day_moving_avg)   AS new_cases_14day_moving_avg,
            SUM(new_deaths_3day_moving_avg)   AS new_deaths_3day_moving_avg,
            SUM(new_deaths_14day_moving_avg)  AS new_deaths_14day_moving_avg,
            SUM(new_cases_5day_moving_avg)    AS new_cases_5day_moving_avg,
            SUM(new_cases_7day_moving_avg)    AS new_cases_7day_moving_avg,
            SUM(new_deaths_5day_moving_avg)   AS new_deaths_5day_moving_avg,
            SUM(new_deaths_7day_moving_avg)   AS new_deaths_7day_moving_avg,
            SUM(total_cases_14day_moving_avg) AS total_cases_14day_moving_avg
        FROM
            {{ref('rpt_le_daily_routing_county_data')}}
        WHERE
            date =
            (
                SELECT
                    MAX(date)
                FROM
                    {{ref('rpt_le_daily_routing_county_data')}})
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12) a
LEFT JOIN
    (
        SELECT
            state,
            initial_claims,
            continued_claims,
            covered_employment,
            insured_unemployment_rate,
            initial_claims_14day_prior,
            continued_claims_14day_prior
        FROM
            {{ref('intm_le_weekly_us_unemployment_claims')}}
        WHERE
            rank = 1) b
ON
    trim(LOWER(a.state)) = trim(LOWER(b.state))