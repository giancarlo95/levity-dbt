WITH source AS (

    SELECT * FROM {{ source('colabel', 'sign_up_view') }}

),

renamed AS (

    SELECT
        *		
    FROM 
        source
        
)

SELECT *
FROM renamed