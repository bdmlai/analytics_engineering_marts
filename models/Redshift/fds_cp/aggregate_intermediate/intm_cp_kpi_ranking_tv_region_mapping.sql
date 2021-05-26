{{
  config({
		
		"materialized": 'ephemeral'
  })
}}

 SELECT
                    country,
                    region,
                    AVG(population) AS population
                FROM
                    (
                        SELECT
                            CASE
                                WHEN country_name='USA'
                                THEN 'United States'
                                ELSE country_name
                            END             AS Country,
                            SUM(population) AS Population
                        FROM
                            {{source('cdm','dim_country_population')}}
                        GROUP BY
                            1) e
                LEFT JOIN
                    (
                        SELECT DISTINCT
                            country_nm,
                            region_nm AS region
                        FROM
                            {{source('cdm','dim_region_country')}}
                        WHERE
                            ent_map_nm = 'GM Region') f
                ON
                    UPPER(e.country)=UPPER(f.country_nm)
                GROUP BY
                    1,2