WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_frontegg_complete','user_table') }}

),

renamed AS (

    SELECT
        frontegg_user_id                     AS user_id,			
        _fivetran_batch,		
        _fivetran_deleted,			
        _fivetran_index,			
        DATE(_fivetran_synced)               AS date_fivetran_synced,			
        created_at                           AS date_user_onboarded,			
        current_frontegg_tenant_id           AS logged_account_id,			
        LOWER(email)                         AS user_email_address,		
        is_locked,			
        is_mfa_enrolled,		
        is_verified,			
        metadata,			
        name                                 AS user_name,			
        provider			 
    FROM source

)

SELECT 
    *,
    CASE 
        WHEN date_fivetran_synced=CURRENT_DATE() THEN 0
        ELSE 1
    END AS is_deleted
FROM renamed
