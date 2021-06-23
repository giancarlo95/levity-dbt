WITH source AS (

    SELECT * FROM {{ source('public', 'production_accounts_userprofile') }}

),

renamed AS (

    SELECT
        id,			
        frontegg_user_id,		
        is_approved,		
        is_service_account,		
        CAST(user_id AS STRING) AS user_id,
        _airbyte_emitted_at,	
        _airbyte_production_accounts_userprofile_hashid  
    FROM source

)

SELECT *
FROM renamed
