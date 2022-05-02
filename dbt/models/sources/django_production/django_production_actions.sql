WITH source AS (

    SELECT * FROM {{ source('django_production', 'tracks_view')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed