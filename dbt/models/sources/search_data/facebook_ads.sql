WITH source AS (

    SELECT * FROM {{ source('pipelinica', 'facebook_ads')}}

), renamed AS (

    SELECT
        *
    FROM 
        source
        
    {# ORDER BY
        year_month DESC #}

)

SELECT *
FROM renamed