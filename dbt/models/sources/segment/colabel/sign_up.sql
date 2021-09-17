WITH source AS (

    SELECT * FROM {{ source('colabel', 'sign_up') }}

),

renamed AS (

    SELECT
        *
    FROM 
        source
        
)

SELECT *
FROM renamed