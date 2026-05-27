SELECT
        dim_year.country,

        (
            SUM(f.nitrous_oxide_mt)
            / SUM(NULLIF(f.total_ghg_mt, 0))
        ) * 100 AS hidden_impact

    FROM 
    
        {{ref('fact_co2_national')}} as f

    JOIN

        {{ref('dim_iso_country_year')}} as dim_year
    ON

        f.iso_code = dim_year.iso_code
    AND
        f.year = dim_year.year

    WHERE 
    
        f.year >= 2001
        
    AND 
    
        f.total_ghg_mt > 0

    GROUP BY dim_year.country

    ORDER BY hidden_impact DESC

    LIMIT 5