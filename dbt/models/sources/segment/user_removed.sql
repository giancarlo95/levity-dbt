WITH source AS (

    SELECT * FROM {{ source('frontegg_1vuiodrwm6cvzejkiqmgp3zna0n','user_removed') }}

),

renamed AS (

    SELECT					
        email           AS user_email_address,			
        event           AS event_name,							
        timestamp       AS date_user_removed,			
        user_id	        AS user_id	
    FROM source

)

SELECT *
FROM renamed