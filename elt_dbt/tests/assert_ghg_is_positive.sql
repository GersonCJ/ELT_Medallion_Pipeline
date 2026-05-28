-- In this singular test, we search data where the emission was negative, which is not a problem, ... 
-- since negative net emission means, gas absorption was greater than gas emission.

SELECT

    iso_code,
    year,
    total_ghg_mt

FROM {{ ref('fact_co2_national') }}

WHERE

    total_ghg_mt < 0