WITH source AS (

    SELECT * FROM {{ source('colabel', 'pages') }}

),

renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed