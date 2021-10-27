WITH source AS (

    SELECT * FROM {{ source('website_try_prod', 'pages_view') }}

),

renamed AS (

    SELECT
        *		
    FROM 
        source
        
)

SELECT *
FROM renamed