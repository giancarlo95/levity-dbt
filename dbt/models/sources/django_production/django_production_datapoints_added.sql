WITH source AS (

    SELECT * FROM {{ source('django_production', 'datapoints_added_view')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed