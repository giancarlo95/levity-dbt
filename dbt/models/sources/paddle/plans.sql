WITH source AS (

    SELECT * FROM {{ source('paddle_users', 'users')}}

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