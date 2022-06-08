WITH source AS (

    SELECT * FROM {{ source('legacy', 'activity')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed