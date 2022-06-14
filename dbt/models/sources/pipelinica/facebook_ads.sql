WITH source AS (

    SELECT * FROM {{ source('pipelinica', 'facebook_ads')}}

), renamed AS (

    SELECT
       *			
    FROM 
        source

)

SELECT *
FROM renamed