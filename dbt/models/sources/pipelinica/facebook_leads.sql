WITH source AS (

    SELECT * FROM {{ source('pipelinica', 'facebook_leads')}}

), renamed AS (

    SELECT
       *			
    FROM 
        source

)

SELECT *
FROM renamed