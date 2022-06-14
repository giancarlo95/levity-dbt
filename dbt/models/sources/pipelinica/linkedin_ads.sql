WITH source AS (

    SELECT * FROM {{ source('pipelinica', 'linkedin_ads')}}

), renamed AS (

    SELECT
       *			
    FROM 
        source

)

SELECT *
FROM renamed