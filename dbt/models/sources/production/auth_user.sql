WITH source AS (

    SELECT * FROM {{ source('public', 'auth_user') }}

),

renamed AS (

    SELECT
        CAST(id AS STRING)              AS user_id,			
        CAST(date_joined AS TIMESTAMP)  AS date_user_onboarded,
        email	                        AS user_email_address,	
        first_name,	
        is_active,		
        is_staff,		
        is_superuser,		
        last_login,		
        last_name,	
        password,		
        username 
    FROM 
        source
    WHERE 
        NOT(email='milanjose999@gmail.com')

)

SELECT *
FROM renamed
