SELECT
        dim_iso.country,
        dim_year.year,

        SUM(f.co2_mt) OVER (
            PARTITION BY dim_iso.iso_code
            ORDER BY dim_year.year
        ) AS cumulative_co2_mt,

        f.temperature_change_from_co2_degrees_c

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
        dim_year.year >= 2000

    ORDER BY dim_iso.country, dim_year.year