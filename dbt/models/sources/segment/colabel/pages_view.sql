WITH source AS (

    SELECT * FROM {{ source('colabel', 'pages_view') }}

),

renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed