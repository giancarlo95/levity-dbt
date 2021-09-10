WITH source AS (

    SELECT * FROM {{ source('frontegg_1vuiodrwm6cvzejkiqmgp3zna0n','users') }}

),

renamed AS (

    SELECT					
        context_library_name,			
        context_library_version,			
        email                                 AS user_email_address,			
        id	                                  AS user_id,		
        loaded_at,		
        received_at,		
        uuid_ts				
    FROM source

)

SELECT *
FROM renamed