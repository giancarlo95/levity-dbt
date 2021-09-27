WITH source AS (

    SELECT * FROM {{ source('colabel', 'button_clicked_view') }}

),

renamed AS (

    SELECT
        *		
    FROM 
        source
        
)

SELECT *
FROM renamed