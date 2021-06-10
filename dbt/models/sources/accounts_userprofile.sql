WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'accounts_userprofile') }}

),

renamed AS (

    SELECT
        id,	
        _fivetran_deleted,		
        _fivetran_synced,		
        frontegg_user_id,		
        is_approved,		
        is_service_account,		
        user_id    
    FROM source

)

SELECT *
FROM renamed
