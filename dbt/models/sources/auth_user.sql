WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'auth_user') }}

),

renamed AS (

    SELECT
        id	                  AS user_id,	
        _fivetran_deleted,		
        _fivetran_synced,		
        date_joined	          AS date_user_onboarded,
        email	              AS user_email_address,	
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
