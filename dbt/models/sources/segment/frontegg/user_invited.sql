WITH source AS (

    SELECT * FROM {{ source('frontegg_1vuiodrwm6cvzejkiqmgp3zna0n','user_invited') }}

),

renamed AS (

    SELECT					
        email           AS user_email_address,			
        event           AS event_name,					
        tenant          AS account_id,			
        timestamp       AS date_user_invited,			
        user_id	        AS user_id	
    FROM source

)

SELECT *
FROM renamed