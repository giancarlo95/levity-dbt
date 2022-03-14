WITH source AS (

    SELECT * FROM {{ source('paddle', 'users')}}

), renamed AS (

    SELECT			
        marketing_consent,				
        paused_at,			
        paused_from,			
        paused_reason,						
        plan_id,					
        signup_date,			
        state,			
        subscription_id,					
        user_email,			
        user_id	
    FROM 
        source

)

SELECT *
FROM renamed