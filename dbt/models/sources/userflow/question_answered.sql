WITH source AS (

    SELECT * FROM {{ source('userflow', 'question_answered_view') }}

),

renamed AS (

    SELECT
        *
    FROM source

)

SELECT *
FROM renamed