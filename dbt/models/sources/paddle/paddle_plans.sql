WITH source AS (

    SELECT * FROM {{ source('paddle', 'plans')}}

), renamed AS (

    SELECT
        id,
        name,
        billing_type,
        billing_period,
        trial_days			
    FROM 
        source

)

SELECT *
FROM renamed