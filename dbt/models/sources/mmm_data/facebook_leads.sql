WITH source AS (

    SELECT * FROM {{ source('pipelinica', 'facebook_leads')}}

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