WITH source AS (

    SELECT * FROM {{ source('colabel', 'form_submitted_view') }}

),

renamed AS (

    SELECT
        *		
    FROM 
        source
        
)

SELECT *
FROM renamed