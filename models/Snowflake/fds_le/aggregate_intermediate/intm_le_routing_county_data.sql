{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    row_number() over (partition BY state, county_name ORDER BY date DESC) AS rank1
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    *,
                    row_number() over(partition BY state,county_fips,date ORDER BY total_cases DESC
                    ) AS rank_covid_dedupe_2
                FROM
                    (
                        SELECT
                            a.date,
                            a.state,
                            a.county AS county_name,
                            a.fips   AS county_fips,
                            a.cases  AS total_cases,
                            a.deaths AS total_deaths,
                            a.iso3166_1,
                            a.iso3166_2        AS state_code,
                            b.total_population AS population,
                            b.total_male_population,
                            b.total_female_population,
                            c.grocery_and_pharmacy_change_perc AS
                                                  grocery_and_pharmacy_percent_change_from_baseline,
                            c.parks_change_perc         AS parks_percent_change_from_baseline,
                            c.residential_change_perc   AS residential_percent_change_from_baseline,
                            c.retail_and_recreation_change_perc AS
                            retail_and_recreation_percent_change_from_baseline,
                            c.transit_stations_change_perc AS
                                                      transit_stations_percent_change_from_baseline,
                            c.workplaces_change_perc AS workplaces_percent_change_from_baseline,
                            d.status_of_reopening,
                            d.stay_at_home_order,
                            d.mandatory_quarantine_for_travelers,
                            d.non_essential_business_closures,
                            d.large_gatherings_ban,
                            d.restaurant_limits,
                            d.bar_closures,
                            d.face_covering_requirement,
                            d.large_gatherings_ban_bucket_group,
                            d.face_covering_requirement_bucket_group
                        FROM
                            {{ref('intm_le_routing_nyt_data')}} a
                        LEFT JOIN
                            {{source('prod_entdwdb.public','demographics')}} b
                        ON
                            trim(a.fips) = trim(b.fips)
                        LEFT JOIN
                            {{source('prod_entdwdb.public','goog_global_mobility_report')}} c
                        ON
                            trim(LOWER(a.iso3166_1)) = trim(LOWER(c.iso_3166_1))
                        AND trim(LOWER(a.iso3166_2)) = trim(LOWER(c.iso_3166_2))
                        AND trim(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(a.county),
                            'county',''),'.',''),'city', ''), 'borough', '') ,'census area', ''),
                            'city and borough', '')) =trim(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE
                            (REPLACE(LOWER(c.sub_region_2),'county',''),'.',''),'city', ''),
                            'borough', '') ,'census area', ''),'city and borough', ''))
                        AND a.date = c.date +
                            (
                                SELECT
                                    (dateADD('day', -1, CURRENT_date) - MAX(date))
                                FROM
                                    {{source('prod_entdwdb.public','goog_global_mobility_report')}}
                            )
                        LEFT JOIN
                            {{ref('rpt_le_daily_kff_state_regulation')}} d
                        ON
                            trim(LOWER(a.state)) = trim(LOWER(d.state))
                        AND a.date = d.state_regulation_update_date))
        WHERE
            rank_covid_dedupe_2 = 1) 