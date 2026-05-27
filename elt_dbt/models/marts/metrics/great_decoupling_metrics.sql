SELECT
        dim_iso.country,
        dim_year.year,
        dim_iso.iso_code,

        dim_year.gdp_usd / 1e9 AS gdp_billions,

        dim_year.population_people,

        f.total_ghg_mt,

        f.total_ghg_mt
            / NULLIF(dim_year.gdp_usd / 1e9, 0)
            AS ghg_intensity

    FROM 
    
        {{ref('fact_co2_national')}} as f

    JOIN

        {{ref('dim_iso_country_year')}} as dim_year
    ON

        f.iso_code = dim_year.iso_code
    AND

        f.year = dim_year.year 
    
    JOIN 

        {{ref('dim_iso_country')}} as dim_iso
    ON

        dim_iso.iso_code = dim_year.iso_code

    WHERE 
        
        dim_iso.country = 'Brazil'
    
    AND 
        
        dim_year.year >= 1996