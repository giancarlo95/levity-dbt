WITH source AS (

    SELECT * FROM {{ source('django_production', 'ai_block_created_view')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed