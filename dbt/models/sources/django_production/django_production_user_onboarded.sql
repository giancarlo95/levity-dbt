WITH source AS (

    SELECT * FROM {{ source('django_production', 'user_onboarded_view')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed