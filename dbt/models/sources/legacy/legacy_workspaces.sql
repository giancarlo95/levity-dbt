WITH source AS (

    SELECT * FROM {{ source('legacy', 'onboarded_accounts')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed