WITH source AS (

    SELECT * FROM {{ source('frontegg_1vuiodrwm6cvzejkiqmgp3zna0n','user_created') }}

),

renamed AS (

    SELECT					
        context_group_id,			
        context_library_name,			
        context_library_version,		
        context_source,			
        email                              AS user_email_address,			
        event	                           AS event_name,		
        event_text,			
        id,			
        loaded_at,			
        original_timestamp,			
        received_at                        AS date_user_created,			
        sent_at,			
        timestamp,			
        user_id                            AS user_id,			
        uuid_ts			
    FROM source

)

SELECT *
FROM renamed