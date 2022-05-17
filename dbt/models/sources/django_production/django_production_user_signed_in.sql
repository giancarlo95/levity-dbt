WITH source AS (

    SELECT * FROM {{ source('django_production', 'user_signed_in_view')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed