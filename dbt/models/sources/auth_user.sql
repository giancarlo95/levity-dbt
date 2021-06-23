WITH source AS (

    SELECT * FROM {{ source('public', 'production_auth_user') }}

),

renamed AS (

    SELECT
        CAST(id AS STRING)              AS user_id,			
        date_joined	                    AS date_user_onboarded,
        email	                        AS user_email_address,	
        first_name,	
        is_active,		
        is_staff,		
        is_superuser,		
        last_login,		
        last_name,	
        password,		
        username,
        _airbyte_emitted_at,	
        _airbyte_production_auth_user_hashid   
    FROM 
        source
    WHERE 
        NOT(email='milanjose999@gmail.com')

)

SELECT *
FROM renamed
