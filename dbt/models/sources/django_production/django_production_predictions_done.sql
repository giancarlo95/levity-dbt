WITH source AS (

    SELECT * FROM {{ source('django_production', 'predictions_done_view')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed