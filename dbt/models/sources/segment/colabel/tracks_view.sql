WITH source AS (

    SELECT * FROM {{ source('colabel', 'tracks_view') }}

),

renamed AS (

    SELECT
        *		
    FROM 
        source
        
)

SELECT *
FROM renamed