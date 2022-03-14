WITH source AS (

    SELECT * FROM {{ source('legacy', 'onboarded_users')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed