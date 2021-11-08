WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'auth_user') }}

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
        username,
        --_airbyte_emitted_at,	
        --_airbyte_production_accounts_paymentplan_hashid
        _fivetran_deleted,
        _fivetran_synced   
    FROM 
        source
    WHERE 
        NOT(email='milanjose999@gmail.com')

)

SELECT *
FROM renamed
